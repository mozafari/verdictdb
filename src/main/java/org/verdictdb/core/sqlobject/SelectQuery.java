/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.base.Optional;

public class SelectQuery extends AbstractRelation implements SqlConvertible {

  //  private boolean isStandardized;

  private static final long serialVersionUID = -4830209213341883527L;

  public static SelectQuery create(SelectItem column) {
    return create(Arrays.asList(column), Arrays.<AbstractRelation>asList());
  }

  public static SelectQuery create(SelectItem column, AbstractRelation relation) {
    return create(Arrays.asList(column), relation);
  }

  public static SelectQuery create(List<SelectItem> columns, AbstractRelation relation) {
    SelectQuery sel = new SelectQuery();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    sel.addTableSource(relation);
    return sel;
  }

  public static SelectQuery create(
      List<SelectItem> columns, AbstractRelation relation, UnnamedColumn predicate) {
    SelectQuery sel = new SelectQuery();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    sel.addTableSource(relation);
    if (predicate != null) {
      sel.filter = Optional.of(predicate);
    }
    return sel;
  }

  public static SelectQuery create(List<SelectItem> columns, List<AbstractRelation> relation) {
    SelectQuery sel = new SelectQuery();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    for (AbstractRelation r : relation) {
      sel.addTableSource(r);
    }
    return sel;
  }

  List<SelectItem> selectList = new ArrayList<>();

  List<AbstractRelation> fromList = new ArrayList<>();

  Optional<UnnamedColumn> filter = Optional.absent();

  List<GroupingAttribute> groupby = new ArrayList<>();

  List<OrderbyAttribute> orderby = new ArrayList<>();

  Optional<UnnamedColumn> having = Optional.absent();

  Optional<UnnamedColumn> limit = Optional.absent();

  Optional<ConstantColumn> useOriginalAfter = Optional.absent();

  //  Optional<String> attribute = Optional.absent();

  /**
   * Copies query specification, i.e., everything except for orderby, having, and limit
   *
   * @return
   */
  @Override
  public SelectQuery deepcopy() {
    SelectQuery sel = new SelectQuery();
    for (SelectItem c : getSelectList()) {
      sel.addSelectItem(c.deepcopy());
    }

    for (AbstractRelation r : getFromList()) {
      sel.addTableSource(r.deepcopy());
    }
    if (getFilter().isPresent()) {
      sel.addFilterByAnd(getFilter().get().deepcopy());
    }
    for (GroupingAttribute a : getGroupby()) {
      sel.addGroupby(a);
    }
    if (getAliasName().isPresent()) {
      sel.setAliasName(getAliasName().get());
    }
    return sel;
  }

  public SelectQuery() {}

  public void addSelectItem(SelectItem column) {
    selectList.add(column);
  }

  public void addTableSource(AbstractRelation relation) {
    fromList.add(relation);
  }

  public void addFilterByAnd(UnnamedColumn predicate) {
    if (!filter.isPresent()) {
      filter = Optional.of(predicate);
    } else {
      filter = Optional.<UnnamedColumn>of(ColumnOp.and(filter.get(), predicate));
    }
  }

  public void addGroupby(GroupingAttribute column) {
    groupby.add(column);
  }

  public void addGroupby(List<GroupingAttribute> columns) {
    groupby.addAll(columns);
  }

  public void addHavingByAnd(UnnamedColumn predicate) {
    if (!having.isPresent()) {
      having = Optional.of(predicate);
    } else {
      having = Optional.<UnnamedColumn>of(ColumnOp.and(having.get(), predicate));
    }
  }

  public void replaceHaving(UnnamedColumn predicate) {
    having = Optional.of(predicate);
  }

  public void clearHaving() {
    having = Optional.absent();
  }

  public void addLimit(UnnamedColumn limit) {
    this.limit = Optional.of(limit);
  }

  public void addUseOriginalAfter(ConstantColumn useOrigAfter) {
    useOriginalAfter = Optional.of(useOrigAfter);
  }

  public void addOrderby(List<OrderbyAttribute> columns) {
    orderby.addAll(columns);
  }

  public void addOrderby(OrderbyAttribute column) {
    orderby.add(column);
  }

  public void setFromList(List<AbstractRelation> fromList) {
    this.fromList = fromList;
  }

  public void clearFilter() {
    this.filter = Optional.absent();
  }

  public void clearFromList() {
    this.fromList = new ArrayList<>();
  }

  public void clearGroupby() {
    this.groupby = new ArrayList<>();
  }

  public void clearOrderBy() {
    this.orderby = new ArrayList<>();
  }

  public void clearSelectList() {
    this.selectList = new ArrayList<>();
  }

  public void clearUseOriginalAfter() {
    this.useOriginalAfter = Optional.absent();
  }

  public List<SelectItem> getSelectList() {
    return selectList;
  }

  public Optional<UnnamedColumn> getFilter() {
    return filter;
  }

  public List<AbstractRelation> getFromList() {
    return fromList;
  }

  public List<GroupingAttribute> getGroupby() {
    return groupby;
  }

  public Optional<UnnamedColumn> getHaving() {
    return having;
  }

  public Optional<UnnamedColumn> getLimit() {
    return limit;
  }

  public List<OrderbyAttribute> getOrderby() {
    return orderby;
  }

  public Optional<ConstantColumn> getUseOriginalAfter() {
    return useOriginalAfter;
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  //  public boolean isStandardized() {
  //    return isStandardized;
  //  }
  //
  //  public void setStandardized() {
  //    isStandardized = true;
  //  }

  // deep copy the select list
  public SelectQuery selectListDeepCopy() {
    List<SelectItem> newSelectItemList = new ArrayList<>();
    for (SelectItem sel : selectList) {
      SelectItem newSel = sel.deepcopy();
      newSelectItemList.add(newSel);
    }
    SelectQuery query = SelectQuery.create(newSelectItemList, fromList);
    query.filter = filter;
    query.groupby = groupby;
    query.orderby = orderby;
    query.having = having;
    query.limit = limit;
    query.aliasName = aliasName;
    return query;
  }
}
