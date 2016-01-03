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

package com.sonian.elasticsearch.action.zookeeper;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class NodesZooKeeperStatusResponse extends BaseNodesResponse<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> implements ToXContent {


    public NodesZooKeeperStatusResponse(ClusterName clusterName, NodeZooKeeperStatusResponse[] nodes) {
        super(clusterName, nodes);
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodes = new NodeZooKeeperStatusResponse[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeZooKeeperStatusResponse.readNodeZooKeeperStatusResponse(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (NodeZooKeeperStatusResponse node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("cluster_name", getClusterNameAsString());

        builder.startObject("nodes");
        for (NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse nodeInfo : nodes) {
            builder.startObject(nodeInfo.getNode().id(), XContentBuilder.FieldCaseConversion.NONE);
            nodeInfo.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();


        return builder;
    }

    public static class NodeZooKeeperStatusResponse extends BaseNodeResponse implements ToXContent {

        boolean enabled = false;

        boolean connected = false;

        NodeZooKeeperStatusResponse() {
        }

        public NodeZooKeeperStatusResponse(DiscoveryNode node, boolean enabled, boolean connected) {
            super(node);
            this.enabled = enabled;
            this.connected = connected;
        }

        public boolean enabled() {
            return enabled;
        }

        public boolean connected() {
            return connected;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            enabled = in.readBoolean();
            connected = in.readBoolean();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(enabled);
            out.writeBoolean(connected);
        }

        public static NodeZooKeeperStatusResponse readNodeZooKeeperStatusResponse(StreamInput in) throws IOException {
            NodeZooKeeperStatusResponse res = new NodeZooKeeperStatusResponse();
            res.readFrom(in);
            return res;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("name", getNode().name());
            builder.field("enabled", enabled());
            builder.field("connected", connected());

            return builder;
        }
    }
}
