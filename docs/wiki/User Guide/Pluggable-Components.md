## Metric Sampler
The metric sampler is one of the most important plugins in Cruise Control, it allows user to easily deploy Cruise Control to various environment and work with the existing metric system.

The default implementation of metric sampler is reading the broker metrics produced by `CruiseControlMetricsReporter` on the broker. This is assuming that users are running Kafka brokers by setting the `metric.reporters` configuration on the Kafka brokers to be `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`(see [quick start](https://github.com/cruise-control-for-kafka/cruise-control#quick-start) on how to do that).

## Metric Sampler Partition Assignor
When users have multiple metric sampler threads, the metric sampler partition assignor is responsible for assigning partitions to the metric samplers.
This is useful when users have some existing metric system. The default implementation assigns all the partitions of the same topic to the same metric sampler.

## Sample Store
The Sample Store is used to store the collected metric samples and training samples to external storage. One problem in metric sampling is that we are using some derived data from the raw metrics. And the way we derive the data relies on the metadata of the cluster at that moment. So when we look at the old metrics, if we do not know the metadata at the point the metric was collected the derived data would be inaccurate. Sample Store helps solve this problem by storing the derived data directly to an external storage for later loading.

The default implementation of Sample Store produces the samples back to the Kafka topic.

## Broker Capacity Config Resolver
The broker Capacity Config Resolver is the way for Cruise Control to get the broker capacity for each of the resources. The default implementation is file based properties. Users can also have a customized implementation to retrieve the capacity of the brokers from some hardware resource management system.

## Goals
The goals in Cruise Control are pluggable with different priorities.

* **Rack-awareness**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner.

 * **RackAwareDistributionGoal** - Contrary to `RackAwareGoal`, as long as replicas of each partition can achieve a perfectly
 even distribution across the racks, this goal lets placement of multiple replicas of a partition into a single rack.

* **ReplicaCapacityGoal**: Attempt to make all the brokers in a cluster to have less than a given number of replicas.

* **CapacityGoals**: Goals that ensures the broker resource utilization is below a given threshold for the corresponding resource. Capacity goals are: 
    * DiskCapacityGoal
    * NetworkInboundCapacityGoal
    * NetworkOutboundCapacityGoal
    * CpuCapacityGoal

* **ReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas. This goal does not take broker capacities into account. In clusters with heterogeneous broker capacities (e.g. brokers with 1G and 10G network), it may prevent the ResourceDistributionGoals from balancing the resource utilization in proportion to broker capacities if it has a higher priority. In such clusters, either remove it from `goals` and `default.goals`, or give it a lower priority than the ResourceDistributionGoals (i.e. list it after them). Since goals cannot undo the balance reached by higher priority goals, it then keeps the capacity-proportional resource distribution, but it will typically report that it could not be satisfied (so do not add it to `anomaly.detection.goals`) and it may still move replicas with low load.

* **PotentialNwOutGoal**: A goal that ensures the potential network output (when all the replicas becomes leaders) on each of the broker do not exceed the broker’s network outbound bandwidth capacity.

* **ResourceDistributionGoals**: Attempt to make the resource utilization variance among all the brokers are within a certain range. Utilization is measured as a percentage of each broker's own capacity, so brokers with more capacity are expected to take proportionally more load -- e.g. with 7G of outbound traffic on two brokers with 1G network and two brokers with 10G network, each broker is expected to be at ~32% of its capacity (i.e. ~0.32G and ~3.2G). The corresponding `*.balance.threshold` configs set the allowed deviation from this cluster-wide average utilization percentage -- not the ratio between the most and the least loaded brokers; hence, setting them to a large value disables balancing rather than allowing more load on brokers with more capacity. In clusters with heterogeneous broker capacities, higher priority goals that ignore broker capacities (e.g. ReplicaDistributionGoal ranked above them, as in the default goal order) can prevent these goals from reaching a capacity-proportional distribution. Note that only leader replicas carry outbound network load: e.g. in a cluster whose brokers have the same disk capacity but different network capacities, DiskUsageDistributionGoal ranked below NetworkOutboundUsageDistributionGoal can still balance disk by moving follower replicas to the brokers with less network capacity. Followers do carry inbound (replication) network load in proportion to the data they store, so a NetworkInboundUsageDistributionGoal ranked above DiskUsageDistributionGoal prevents this; rank it below (or leave it out). Brokers that host many followers may receive many leaders if a broker with more capacity fails, which PotentialNwOutGoal reports as a violation. This goal does not do anything if the cluster is in a low utilization mode (when all the resource utilization of each broker is below a configured percentage.) This is not a single goal, but consists of the following separate goals for each of the resources. 
    * DiskUtilDistributionGoal
    * NetworkInboundUtilDistributionGoal
    * NetworkOutboundUtilDistributionGoal
    * CpuUtilDistributionGoal

* **TopicReplicaDistributionGoal**: Attempt to make the replicas of the same topic evenly distributed across the entire cluster.

* **LeaderReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of leader replicas. This goal does not take broker capacities into account. In clusters with heterogeneous broker capacities (e.g. brokers with 1G and 10G network), it may prevent the ResourceDistributionGoals from balancing the resource utilization in proportion to broker capacities if it has a higher priority. In such clusters, either remove it from `goals` and `default.goals`, or give it a lower priority than the ResourceDistributionGoals (i.e. list it after them). Since goals cannot undo the balance reached by higher priority goals, it then keeps the capacity-proportional resource distribution, but it will typically report that it could not be satisfied (so do not add it to `anomaly.detection.goals`) and it may still move replicas with low load.

* **LeaderBytesInDistributionGoal**: Attempt to make the leader bytes in rate on each host to be balanced. This goal does not take broker capacities into account.

* **PreferredLeaderElectionGoal**: Attempt to make the first replica in replica list leader replica of the partition for all topic partition.

* **KafkaAssignerGoals**: These goals are used to make Cruise Control behaves like [Kafka assigner tool](https://github.com/linkedin/kafka-tools/wiki/Kafka-Assigner). These goals will be picked up if `kafka_assigner` parameter is set to true in corresponding request(e.g. [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).
    * **KafkaAssignerDiskUsageDistributionGoal**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner. 
    * **KafkaAssignerEvenRackAwareGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **IntraBrokerDiskCapacityGoal**: Goals that ensures the disk resource utilization is below a given threshold. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance).

* **IntraBrokerDiskUsageDistributionGoal**: Attempt to make the utilization variance among all the disks within same broker are within a certain range. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance).

* **IntraBrokerDiskIORateDistributionGoal**: Attempt to make the estimated disk I/O rate of all the disks within same broker are within a certain range (determined by `intra.broker.disk.io.balance.threshold`, which falls back to `disk.balance.threshold` if not set) around the average disk I/O rate of the disks of the broker. The disk I/O rate of a replica is estimated as `NW_IN + w * NW_OUT`, where `NW_IN` approximates disk writes (for both leader and follower replicas, as each replica writes the produced bytes to its disk), `NW_OUT` approximates disk reads (for leader replicas only), and `w` is `intra.broker.disk.io.read.weight` (default: `1.0`). Consider lowering `w` (e.g. `0.3`) if most reads are served from the page cache (e.g. consumers reading from the tail of the log), so that disks hosting write-heavy followers are not considered as loaded as disks hosting leaders of topics with many consumers. This goal is not used by default; to use it when `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance), add it to `intra.broker.goals` before `IntraBrokerDiskUsageDistributionGoal`, e.g. in the order `IntraBrokerDiskCapacityGoal`, `IntraBrokerDiskIORateDistributionGoal`, `IntraBrokerDiskUsageDistributionGoal`. Goals are optimized in the order of priority, and a goal can only take actions that the goals optimized before it accept. Placed after `IntraBrokerDiskUsageDistributionGoal`, this goal can do little, since most of its moves would push the disk usage of a disk out of the limits of `IntraBrokerDiskUsageDistributionGoal`. Placed before it, disk I/O balance takes precedence over disk usage balance, so `IntraBrokerDiskUsageDistributionGoal` may end up violated. Not available in `kafka_0_11_and_1_0` branch.

* **BrokerSetAwareGoal**: BrokerSet is defined as a subset of brokers in the cluster. This Goal will Attempt to make the replica movements constrained within the boundary of a BrokerSet.
  The [replica to broker set mapping policy](#replica-to-broker-set-mapping-policy) decides which broker set each replica belongs to, or that a replica is not bound to any broker set. The goal:
    * moves a replica that is outside its broker set, or offline, to an alive broker of its broker set, and fails the optimization if it cannot;
    * rejects any later move or swap that would take a replica out of its broker set (leadership moves are always accepted);
    * after optimization, checks that each topic whose replicas are all bound lives inside one broker set, and that each bound replica lives in its own broker set;
    * leaves replicas that are not bound to any broker set alone: they are moved only if they are offline (to any alive broker), are never vetoed and are never checked.

  Topics in `excluded_topics`, `topics.excluded.from.partition.movement` or `topics.with.min.leaders.per.broker` are neither moved nor checked by this goal. Put this goal first in `default.goals` and `hard.goals`, so that later goals are vetoed instead of undone.

## Anomaly Notifier
The anomaly notifier is a communication channel between Cruise Control and users.
It notifies users about the anomalies detected in the cluster as well as actions taken about the anomaly. Anomalies include:
* Broker failure
* Goal violation
* Metric Anomaly
* Disk failure
* Topic Anomaly

The actions users can take are:
* Fix the anomaly
* Wait some time and check if the anomaly still exists
* Ignore the anomaly

By default Cruise Control is configured to use `NoopNotifier` which ignores all the anomalies.

## Replica Movement Strategy
The strategy to determine the execution order for generated proposals. By default `BaseReplicaMovementStrategy` is used, which is totally random.
Sometimes this could result in prolonged execution time due to some long tail tasks in each execution batches. Other available strategies includes:
* **PrioritizeSmallReplicaMovementStrategy**: prioritize small sized replicas
* **PrioritizeLargeReplicaMovementStrategy**: prioritize large sized replicas
* **PostponeUrpReplicaMovementStrategy**: prioritize replicas for partition having no out-of-sync replica
* **PrioritizeMinIsrWithOfflineReplicasStrategy**: prioritize tasks with (At/Under)MinISR partitions with offline replicas

The strategies can be chained to use and can be dynamically set using `replica_movement_strategies` in corresponding request(e.g. [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).

## Maintenance Event Reader
When a system/tool in Kafka Ecosystem has knowledge about an upcoming planned maintenance, such as a switch change, an upcoming VM freeze, 
or reboot of VMs on cloud deployments, the desired preventative mitigation strategy (i.e. `maintenance event`) can be conveyed to 
Cruise Control for automated execution of it without human intervention.

Such maintenance events correspond to admin operations that the relevant system/tool submits to Cruise Control via a generic maintenance event store.
Then, Maintenance Event Reader retrieves the maintenance events from the relevant event store for automated execution.

The default implementation of Maintenance Event Reader (see `MaintenanceEventTopicReader`) consumes maintenance events from a Kafka topic.

## Provisioner
This component provides the means for adding or removing resources to / from the cluster.

Resources include:
* Brokers that can host replicas 
* Racks containing brokers that can host replicas
* Disks (i.e. relevant to JBOD deployments)
* Partitions of a topic (can only be added)

Constraints include:
* Topic name: If the resource is partition, the name of the topic must be specified.
* A typical broker id and its capacity (one cannot be specified without the other)
* Specific resource, such as {@link Resource#DISK}.
* Excluded racks -- i.e. racks for which brokers should not be added to or removed from
* Total resource capacity required to add or remove

## Broker Set Resolver
The broker set resolver is the way for Cruise Control to get the broker set information for all the brokers. The default implementation is file based properties. Users can also have a customized implementation to retrieve the broker set to broker mapping from some other source.

## Default Broker Set Assignment Policy
The default broker set assignment policy handles the case where a broker id is not provided with any mapping to a broker set. There is a default NoOpDefaultBrokerSetAssignmentPolicy that can be used if there is no default mapping and any mismatch will result in fast failure. Users can have a customized implementation to provide a default treatment for unmapped brokers.

## Replica To Broker Set Mapping Policy
The replica to broker set mapping policy (`replica.to.broker.set.mapping.policy.class`) maps each replica to the broker set that BrokerSetAwareGoal keeps it in. A policy may return `null` for a replica, which leaves that replica unconstrained. The built-in policies are:
* **TopicNameHashBrokerSetMappingPolicy** (default): maps all replicas of a topic to a broker set chosen by a consistent hash of the topic name over the sorted broker set ids. Every topic is bound to a broker set.
* **ReplicaToOriginalBrokerSetMappingPolicy**: maps a replica to the broker set of the broker that originally hosts it, i.e. it keeps each replica in the broker set it is already in.
* **TopicPatternBrokerSetMappingPolicy**: maps all replicas of a topic to the broker set whose optional `topicPattern` (a regular expression in `broker.set.config.file`) fully matches the topic name, and leaves topics that match no pattern unconstrained. The file is re-read on each optimization, so changes apply without a restart. The optimization fails if a pattern does not compile, if a topic matches the patterns of more than one broker set, or if a matched broker set is not known to the broker set resolver.

### Example: keep a topic on the 10G brokers
In a cluster where broker 1 has a 1G network and brokers 2 and 3 have a 10G network, the following keeps the replicas of topic `hot` and of topics starting with `orders-` on brokers 2 and 3, while all other topics stay free to move anywhere. Broker 1 is not listed, so the default NoOpBrokerSetAssignmentPolicy puts it in the `unmapped` broker set.

`config/brokerSets.json`:
```json
{
  "brokerSets": [
    {
      "brokerSetId": "10G",
      "brokerIds": [2, 3],
      "topicPattern": "hot|orders-.*",
      "doc": "Topics matching topicPattern may only live on brokers 2 and 3"
    }
  ]
}
```

`config/cruisecontrol.properties`:
```properties
replica.to.broker.set.mapping.policy.class=com.linkedin.kafka.cruisecontrol.config.TopicPatternBrokerSetMappingPolicy
broker.set.config.file=config/brokerSets.json
# BrokerSetAwareGoal FIRST, so later goals are vetoed instead of undone
default.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal,<the existing default goals>
hard.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal,<the existing hard goals>
# also add it to anomaly.detection.goals / self.healing.goals if self-healing must keep the pin
```

With this configuration every rebalance moves the replicas of the pinned topics off broker 1 and never moves them back. If the pinned broker set cannot host a replica (for example, too few alive brokers for the replication factor, too few racks for RackAwareGoal, all of its brokers excluded, or `destination_broker_ids` / `add_broker` limiting the destinations to brokers outside the set), the optimization fails with an error that names the broker set.
