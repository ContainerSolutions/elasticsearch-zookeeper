/*
 * Copyright 2011 Sonian Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sonian.elasticsearch.zookeeper.discovery;

import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperIncompatibleStateVersionException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class ZooKeeperClusterStateTests extends AbstractZooKeeperTests {
    @BeforeClass
    public void createTestPaths() throws Exception {
        startZooKeeper();
    }

    @AfterClass
    public void shutdownZooKeeper() {
        stopZooKeeper();
    }

    public ZooKeeperClusterStateTests() {
        putDefaultSettings(Settings.settingsBuilder().put("zookeeper.maxnodesize", 10).build());
    }

    @Test
    public void testClusterStatePublishing() throws Exception {

        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);
        ZooKeeperClusterState zkState = buildZooKeeperClusterState(nodes);

        zkState.start();

        zkState.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        final CountDownLatch latch = new CountDownLatch(1);

        ClusterState retrievedState = zkState.retrieve(new ZooKeeperClusterState.NewClusterStateListener() {

            @Override
            public void onNewClusterState(ClusterState clusterState) {
                latch.countDown();
            }
        });

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(initialState)));

        ClusterState secondVersion = ClusterState.builder(initialState)
                .version(1235L)
                .build();

        zkState.publish(new ClusterChangedEvent("", secondVersion, initialState), new NoOpAckListener());

        retrievedState = zkState.retrieve(null);

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(secondVersion)));

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        zkState.stop();

    }

    @Test
    public void testClusterStatePublishingWithDifferentMinorVersion() throws Exception {
        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);

        ZooKeeperClusterState zkStateOld = buildZooKeeperClusterState(nodes, Version.V_1_1_0);

        zkStateOld.start();

        zkStateOld.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        zkStateOld.stop();

        ZooKeeperClusterState zkStateNew = buildZooKeeperClusterState(nodes, Version.V_1_3_1);

        zkStateNew.start();

        zkStateNew.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        try {
            zkStateNew.retrieve(null);
            assertThat("Should read the state stored by the same minor version", true);
        } catch (ZooKeeperIncompatibleStateVersionException ex)
        {
            assertThat("Should read the state stored by the same minor version", false);
        }

        zkStateNew.stop();
    }

    @Test
    public void testClusterStatePublishingWithNewVersion() throws Exception {
        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);

        ZooKeeperClusterState zkStateOld = buildZooKeeperClusterState(nodes, Version.V_0_18_0);

        zkStateOld.start();

        zkStateOld.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        zkStateOld.stop();

        ZooKeeperClusterState zkStateNew = buildZooKeeperClusterState(nodes, Version.V_1_2_0);

        zkStateNew.start();

        try {
            zkStateNew.retrieve(null);
            assertThat("Shouldn't read the state stored by a different version", false);
        } catch (ZooKeeperIncompatibleStateVersionException ex) {
            assertThat(ex.getMessage(), is("Local version: 1.2.0 incompatible with remote version: 0.18.0"));
        }
        ZooKeeperClient zk = buildZooKeeper();

        // Make sure that old state wasn't deleted
        assertThat(zk.getNode(zooKeeperEnvironment().statePartsNodePath(), null), notNullValue());

        zkStateNew.syncClusterState();

        // Make sure that new start can be published now
        zkStateNew.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        zkStateNew.stop();

    }

    @Test
    public void testReadOldPluginClusterState() throws Exception {
        // ensure we can read state from versions of plugin between 1.3.1 and version serialization change
        testReadOldPluginClusterState("1.1");
    }

    @Test
    public void testReadOlderPluginClusterState() throws Exception {
        // ensure we can read state from pre-1.3.1 versions of this plugin
        testReadOldPluginClusterState("1.1.0");
    }

    public void testReadOldPluginClusterState(String version) throws Exception {
        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);

        ZooKeeperClusterState zkStateOld = buildZooKeeperClusterState(nodes, version);

        zkStateOld.start();

        zkStateOld.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        zkStateOld.stop();

        ZooKeeperClusterState zkStateNew = buildZooKeeperClusterState(nodes, Version.V_1_4_1);

        zkStateNew.start();

        zkStateNew.publish(new ClusterChangedEvent("", initialState, null), new NoOpAckListener());

        try {
            zkStateNew.retrieve(null);
            assertThat("Should read the state stored by the same minor version", true);
        } catch (ZooKeeperIncompatibleStateVersionException ex)
        {
            assertThat("Should read the state stored by the same minor version", false);
        }

        zkStateNew.stop();
    }

    @Test
    public void testReadOldClusterState() throws Exception {
        ZooKeeperClient zk = buildZooKeeper();
        zk.start();
        String statePath = "/es/clusters/test-cluster-local/state";
        zk.createPersistentNode(statePath);
        final BytesStreamOutput buf = new BytesStreamOutput();
        buf.setVersion(Version.V_1_1_0);
        buf.writeString(Version.V_1_1_0.number());
        buf.writeLong(1234);
        for (String part : Arrays.asList("routingTable", "discoveryNodes", "metaData", "clusterBlocks")) {
            String path = zk.createLargeSequentialNode(statePath + "/" + part + "_",
                    new InputStreamStreamInput(getClass().getResourceAsStream(part)).readByteArray());
            buf.writeString(path);
        }
        zk.setOrCreatePersistentNode(statePath + "/parts", buf.bytes().copyBytesArray().toBytes());
        zk.stop();

        ZooKeeperClusterState zkState = buildZooKeeperClusterState(testDiscoveryNodes());
        zkState.start();

        ClusterState state = zkState.retrieve(null);
        assertThat(state.getRoutingTable(), notNullValue());
        assertThat(state.getMetaData().getCustoms().get("repositories"), notNullValue());

        // check that state was serialized correctly with new version
        zkState.publish(new ClusterChangedEvent("", state, null), new NoOpAckListener());
        zkState.stop();

        ZooKeeperClusterState zkStateUpdated = buildZooKeeperClusterState(testDiscoveryNodes());
        zkStateUpdated.start();
        state = zkStateUpdated.retrieve(null);
        assertThat(state.getMetaData().getCustoms().get("repositories"), notNullValue());
        zkStateUpdated.stop();
    }
}
