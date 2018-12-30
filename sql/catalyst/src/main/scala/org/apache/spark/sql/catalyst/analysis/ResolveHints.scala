/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.internal.SQLConf


/**
 * Collection of rules related to hints. The only hint currently available is broadcast join hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from broadcast.
 */
object ResolveHints {
  /**
   * For broadcast hint, we accept "BROADCAST", "BROADCASTJOIN", and "MAPJOIN", and a sequence of
   * relation aliases can be specified in the hint. A broadcast hint plan node will be inserted
   * on top of any relation (that is not aliased differently), subquery, or common table expression
   * that match the specified name.
   *
   * The hint resolution works by recursively traversing down the query plan to find a relation or
   * subquery that matches one of the specified broadcast aliases. The traversal does not go past
   * beyond any existing broadcast hints, subquery aliases.
   *
   * This rule must happen before common table expressions.
   */
  class ResolveBroadcastHints(conf: SQLConf) extends Rule[LogicalPlan] {
    private val BROADCAST_HINT_NAMES = Set("BROADCAST", "BROADCASTJOIN", "MAPJOIN")

    // SKEWED_JOIN(join_pair(left.field, right.field), skewed_keys('key1', 'key2'))
    private val SKEWED_JOIN = "SKEWED_JOIN"

    def resolver: Resolver = conf.resolver

    private def applyBroadcastHint(plan: LogicalPlan, toBroadcast: Set[String]): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case u: UnresolvedRelation if toBroadcast.exists(resolver(_, u.tableIdentifier.table)) =>
            ResolvedHint(plan, HintInfo(broadcast = true))
          case r: SubqueryAlias if toBroadcast.exists(resolver(_, r.alias)) =>
            ResolvedHint(plan, HintInfo(broadcast = true))

          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing broadcast hint, there is no point going down (if we do, we either
            // won't change the structure, or will introduce another broadcast hint that is useless.
            // The rest (view, with, subquery) indicates different scopes that we shouldn't traverse
            // down. Note that technically when this rule is executed, we haven't completed view
            // resolution yet and as a result the view part should be deadcode. I'm leaving it here
            // to be more future proof in case we change the view we do view resolution.
            recurse = false
            plan

          case _ =>
            plan
        }
      }

      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren(child => applyBroadcastHint(child, toBroadcast))
      } else {
        newNode
      }
    }

    private def getTbAlias(plan: LogicalPlan, tableName: String): String = {
      plan.map(lp => lp)
        .filter(_.isInstanceOf[SubqueryAlias])
        .map(_.asInstanceOf[SubqueryAlias])
        .filter(_.child.isInstanceOf[UnresolvedRelation])
        .filter{ sa =>
          sa.child.asInstanceOf[UnresolvedRelation].tableName == tableName
        }.map(s => s"${s.alias}.").headOption.getOrElse("")
    }

    private def applySkewedJoinHint(plan: LogicalPlan, skewedJoin: SkewedJoin): LogicalPlan = {
      // scalastyle:off println
      var recurse = true
      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case Join(left, right, joinType, condition) if condition.isDefined =>
            val joinPair = skewedJoin.joinPair
            val hasLeftTb = left.find { lp =>
                  lp.isInstanceOf[UnresolvedRelation] &&
                  lp.asInstanceOf[UnresolvedRelation].tableName == joinPair.leftTable
                }.isDefined

            val hasRightTb = right.find { lp =>
                  lp.isInstanceOf[UnresolvedRelation] &&
                  lp.asInstanceOf[UnresolvedRelation].tableName == joinPair.rightTable
                }.isDefined

            val leftField = getTbAlias(left, joinPair.leftTable) + joinPair.leftField
            val rightField = getTbAlias(right, joinPair.rightTable) + joinPair.rightField
            val joinKeys = condition.get.map(expr => expr)
                  .filter(_.isInstanceOf[UnresolvedAttribute])
                  .map(_.asInstanceOf[UnresolvedAttribute].name)
                  .filter(n => n.endsWith(joinPair.leftField) || n.endsWith(joinPair.rightField))

            val newPlan = if (hasLeftTb && hasRightTb && joinKeys.length >= 2) {
              val inList = skewedJoin.skewedKeys.map(Literal(_))
              val left1 = Filter(Not(In(UnresolvedAttribute(leftField), inList)), left)
              val right1 = Filter(Not(In(UnresolvedAttribute(rightField), inList)), right)
              val left2 = Filter(In(UnresolvedAttribute(leftField), inList), left)
              val right2 = Filter(In(UnresolvedAttribute(rightField), inList), right)

              val join1 = Join(left1, right1, joinType, condition)
              // use mapjoin
              val join2 = Join(ResolvedHint(left2, HintInfo(broadcast = true)),
                            ResolvedHint(right2, HintInfo(broadcast = true)),
                            Inner, condition)
              Union(Seq(join1, join2))
            } else plan
            ResolvedHint(newPlan)
          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            recurse = false
            plan

          case _ =>
            plan
        }
      }
      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren(child => applySkewedJoinHint(child, skewedJoin))
      } else {
        newNode
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = {
      var castFieldId: Set[String] = Set()

      val newNode = plan transformUp {
        case h: UnresolvedHint if BROADCAST_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
          if (h.parameters.isEmpty) {
            // If there is no table alias specified, turn the entire subtree into a BroadcastHint.
            ResolvedHint(h.child, HintInfo(broadcast = true))
          } else {
            // Otherwise, find within the subtree query plans that should be broadcasted.
            applyBroadcastHint(h.child, h.parameters.map {
              case tableName: String => tableName
              case tableId: UnresolvedAttribute => tableId.name
              case unsupported => throw new AnalysisException("Broadcast hint parameter should be" +
                s" an identifier or string but was $unsupported (${unsupported.getClass}")
            }.toSet)
          }

        case h: UnresolvedHint if SKEWED_JOIN == h.name.toUpperCase(Locale.ROOT) =>
          val paramMap = h.parameters.map {
            case UnresolvedFunction(funId, children, _) =>
              (funId.funcName, children.map {
                case ua: UnresolvedAttribute => ua.name
                case other => other.toString
              })
            case unsupported => throw new AnalysisException("SKEWED hint parameter should be" +
              s" Function but was $unsupported (${unsupported.getClass}")
          }.toMap
          val joinPair = paramMap.get("join_pair")
          val skewedKeys = paramMap.get("skewed_keys")
          if (joinPair.nonEmpty && joinPair.get.length == 2
            && skewedKeys.nonEmpty && skewedKeys.get.length > 0) {
            applySkewedJoinHint(h.child,
              SkewedJoin(JoinPair(joinPair.get(0), joinPair.get(1)), skewedKeys.get))
          } else {
            ResolvedHint(h.child)
          }
      }
      newNode
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  object RemoveAllHints extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case h: UnresolvedHint => h.child
    }
  }

  case class JoinPair(leftTbField: String, rightTbField: String) {
    val leftTable: String = leftTbField.split("\\.")(0)
    val leftField: String = leftTbField.split("\\.")(1)
    val rightTable: String = rightTbField.split("\\.")(0)
    val rightField: String = rightTbField.split("\\.")(1)
  }
  case class SkewedJoin(joinPair: JoinPair, skewedKeys: Seq[String])
}
