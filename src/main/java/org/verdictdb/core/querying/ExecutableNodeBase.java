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

package org.verdictdb.core.querying;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.execplan.ExecutableNodeRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;
import org.verdictdb.core.execplan.MethodInvocationInformation;
import org.verdictdb.core.querying.ola.AggMeta;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class ExecutableNodeBase implements ExecutableNode, Serializable {

  private static final long serialVersionUID = 1424215482199124961L;

  /**
   *  parent; the tokens are broadcasted to these nodes
   */
  List<ExecutableNodeBase> subscribers = new ArrayList<>();

  /**
   *  pairs of a source and the channel; see below for channel
   */
  List<Pair<ExecutableNodeBase, Integer>> sources = new ArrayList<>();

  /**
   *  pairs of channel ID and the queue for containing broadcasted tokens.
   */
  Map<Integer, ExecutionTokenQueue> channels = new TreeMap<>();

  protected AggMeta aggMeta = new AggMeta();

  protected int uniqueId;

  private int groupId; // copied when deepcopying; used by ExecutablePlanRunner
  
  private UniqueChannelCreator channelCreator = new UniqueChannelCreator(this);
  
  private ExecutableNodeRunner runner;

  public ExecutableNodeBase(IdCreator creator) {
    this(creator.generateSerialNumber());
  }
  
  /**
   *
   * @param uniqueId -1 indicates 'not assinged'
   */
  public ExecutableNodeBase(int uniqueId) {
    this.uniqueId = uniqueId;
    groupId = Integer.valueOf(RandomStringUtils.randomNumeric(5));
  }
  
  public void setId(int id) {
    uniqueId = id;
  }
  
  public int getId() {
    return uniqueId;
  }

//  public static ExecutableNodeBase create() {
//    return new ExecutableNodeBase();
//  }

  public int getGroupId() {
    return groupId;
  }

  // setup method
  public SubscriptionTicket createSubscriptionTicket() {
    int channelNumber = channelCreator.getNewChannelNumber();
    return new SubscriptionTicket(this, channelNumber);
  }

  public void registerSubscriber(SubscriptionTicket ticket) {
    if (ticket.getChannel().isPresent()) {
      ticket.getSubscriber().subscribeTo(this, ticket.getChannel().get());
    } else {
      ticket.getSubscriber().subscribeTo(this);
    }
  }

  public void subscribeTo(ExecutableNodeBase node) {
    for (int channel = 0; ; channel++) {
      if (!channels.containsKey(channel)) {
        subscribeTo(node, channel);
        break;
      }
    }
  }

  public void subscribeTo(ExecutableNodeBase node, int channel) {
    //    node.getSubscribers().add(this);
    node.addSubscriber(this);
    sources.add(Pair.of(node, channel));
    if (!channels.containsKey(channel)) {
      channels.put(channel, new ExecutionTokenQueue());
    }
  }

  private void addSubscriber(ExecutableNodeBase node) {
    subscribers.add(node);
  }

  /**
   * Removes node from the subscription list (i.e., sources).
   *
   * @param node
   * @return The channel via which the removed node was previously subscribed.
   */
  public int cancelSubscriptionTo(ExecutableNodeBase node) {
    List<Pair<ExecutableNodeBase, Integer>> newSources = new ArrayList<>();
    Set<Integer> leftChannels = new HashSet<>();
    int originalChannel = -1;
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      if (!s.getLeft().equals(node)) {
        newSources.add(s);
        leftChannels.add(s.getRight());
        continue;
      } else {
        originalChannel = s.getRight();
      }
    }
    sources = newSources;

    // if there are no other nodes broadcasting to this channel, remove the queue
    if (leftChannels.size() > 0) {
      for (Integer c : leftChannels) {
        if (!channels.containsKey(c)) {
          channels.remove(c);
        }
      }
    } else { // the parent has only one child, so just remove the channel
      channels.clear();
    }

    // inform the node
    node.removeSubscriber(this);
    
    return originalChannel;
  }

  private void removeSubscriber(ExecutableNodeBase node) {
    subscribers.remove(node);
  }

  public void cancelSubscriptionsFromAllSubscribers() {
    // make a copied list of subscribers (to avoid concurrent modifications
    List<ExecutableNodeBase> copiedSubscribiers = new ArrayList<>();
    for (ExecutableNodeBase s : subscribers) {
      copiedSubscribiers.add(s);
    }

    // now cancel subscriptions
    for (ExecutableNodeBase s : copiedSubscribiers) {
      s.cancelSubscriptionTo(this);
    }
    //    subscribers = new ArrayList<>();
  }

  // runner methods
  @Override
  public void getNotified(ExecutableNode source, ExecutionInfoToken token) {
    //    System.out.println("get notified: " + source + " " + token);
    for (Pair<ExecutableNodeBase, Integer> a : sources) {
      if (source.equals(a.getLeft())) {
        int channel = a.getRight();
        channels.get(channel).add(token);
        //    System.out.println("channel: " + channel);
        //    System.out.println("get notified: " + token);
      }
    }
  }

  @Override
  public Map<Integer, ExecutionTokenQueue> getSourceQueues() {
    return channels;
  }
  
//  public void replaceSubscriber(
//      ExecutableNodeBase oldSubscriber, 
//      ExecutableNodeBase newSubscriber) {
//    List<ExecutableNodeBase> newSubscribers = new ArrayList<>();
//    for (ExecutableNodeBase n : subscribers) {
//      if (n.equals(oldSubscriber)) {
//        newSubscribers.add(newSubscriber);
//      } else {
//        newSubscribers.add(n);
//      }
//    }
//    subscribers = newSubscribers;
//  }
  
  public void replaceSource(ExecutableNodeBase oldSource, ExecutableNodeBase newSource) {
    int subscribedChannel = cancelSubscriptionTo(oldSource);
    if (subscribedChannel > 0) {
      // positive channel indicates the old source existed in the source list
      subscribeTo(newSource, subscribedChannel);
    }
//    List<Pair<ExecutableNodeBase, Integer>> newSources = new ArrayList<>();
//    for (Pair<ExecutableNodeBase, Integer> sourceChannel : sources) {
//      ExecutableNodeBase source = sourceChannel.getLeft();
//      Integer channel = sourceChannel.getRight();
//      if (source.equals(oldSource)) {
//        newSources.add(Pair.of(newSource, channel));
//      } else {
//        newSources.add(sourceChannel);
//      }
//    }
//    sources = newSources;
  }

  @Override
  public List<ExecutableNode> getSubscribers() {
    List<ExecutableNode> nodes = new ArrayList<>();
    for (ExecutableNodeBase s : subscribers) {
      nodes.add(s);
    }
    return nodes;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return null;
  }

  @Override
  public int getDependentNodeCount() {
    return sources.size();
  }

  @Override
  public Map<String, MethodInvocationInformation> getMethodsToInvokeOnConnection() {
    return new HashMap<>();
  }

  // Helpers
  public List<ExecutableNodeBase> getSources() {
    List<Pair<ExecutableNodeBase, Integer>> temp = getSourcesAndChannels();
    Collections.sort(
        temp,
        new Comparator<Pair<ExecutableNodeBase, Integer>>() {
          @Override
          public int compare(
              Pair<ExecutableNodeBase, Integer> o1, Pair<ExecutableNodeBase, Integer> o2) {
            return o1.getRight() - o2.getRight();
          }
        });

    List<ExecutableNodeBase> ss = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Integer> s : temp) {
      ss.add(s.getKey());
    }

    return ss;
  }
  
  public int getSourceCount() {
    return getSources().size();
  }

  public Integer getChannelForSource(ExecutableNodeBase node) {
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      if (s.getLeft().equals(node)) {
        return s.getRight();
      }
    }
    return null;
  }

  public List<Pair<ExecutableNodeBase, Integer>> getSourcesAndChannels() {
    List<Pair<ExecutableNodeBase, Integer>> sourceAndChannel = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      sourceAndChannel.add(Pair.of(s.getKey(), s.getValue()));
    }
    return sourceAndChannel;
  }

  public List<ExecutableNodeBase> getExecutableNodeBaseParents() {
    List<ExecutableNodeBase> parents = new ArrayList<>();
    for (ExecutableNode node : subscribers) {
      parents.add((ExecutableNodeBase) node);
    }
    return parents;
  }

  public List<ExecutableNodeBase> getExecutableNodeBaseDependents() {
    return getSources();
    //    List<ExecutableNodeBase> deps = new ArrayList<>();
    //    for (ExecutableNode node : sources.keySet()) {
    //      deps.add((ExecutableNodeBase) node);
    //    }
    //    return deps;
  }

  public ExecutableNodeBase getExecutableNodeBaseDependent(int idx) {
    return getExecutableNodeBaseDependents().get(idx);
  }

  public ExecutableNodeBase deepcopy() {
    ExecutableNodeBase node = new ExecutableNodeBase(uniqueId);
    copyFields(this, node);
    return node;
  }

  protected void copyFields(ExecutableNodeBase from, ExecutableNodeBase to) {
    to.subscribers = new ArrayList<>(from.subscribers);
    to.sources = new ArrayList<>(from.sources);
    to.channels = new TreeMap<>();
    for (Entry<Integer, ExecutionTokenQueue> a : from.channels.entrySet()) {
      to.channels.put(a.getKey(), new ExecutionTokenQueue());
    }
    to.groupId = from.groupId;
    //    to.channels = new TreeMap<>(from.channels);
  }

  public void print() {
    System.out.println(print(0));
  }
  
  public String getStructure() {
    return print(0);
  }

  private String print(int indentSpace) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < indentSpace; i++) {
      builder.append(" ");
    }
    builder.append(this.toString());
    builder.append("\n");
//    System.out.println(builder.toString());

    for (ExecutableNodeBase dep : getExecutableNodeBaseDependents()) {
      builder.append(dep.print(indentSpace + 2));
    }
    
    return builder.toString();
  }

  public AggMeta getAggMeta() {
    return aggMeta;
  }

  public void setAggMeta(AggMeta aggMeta) {
    this.aggMeta = aggMeta;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(uniqueId).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    ExecutableNodeBase rhs = (ExecutableNodeBase) obj;
    return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(uniqueId, rhs.uniqueId)
        .isEquals();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("subscriberCount", subscribers.size())
        //        .append("sources", sources)
        .append("sourceCount", sources.size())
        //        .append("channels", channels)
        //        .append("channels", channels)
        .toString();
  }
  
  @Override
  public void registerNodeRunner(ExecutableNodeRunner runner) {
    this.runner = runner;
  }

  @Override
  public ExecutableNodeRunner getRegisteredRunner() {
    return runner;
  }
}

class UniqueChannelCreator implements Serializable {
  
  private static final long serialVersionUID = -344656097467066883L;

  private int identifierNum = 0;
  
  private ExecutableNodeBase node;
  
  public UniqueChannelCreator(ExecutableNodeBase node) {
    this.node = node;
  }
  
  public int getNewChannelNumber() {
    // 1000 is an arbitrary number
    int newNumber = node.getId()*1000 + identifierNum;
    identifierNum++;
    return newNumber;
  }
  
}
