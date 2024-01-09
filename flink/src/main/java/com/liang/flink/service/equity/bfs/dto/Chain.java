package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.math.BigDecimal.ZERO;

@Data
public class Chain implements Serializable {
    private final List<Object> path = new ArrayList<>();
    private final Set<String> ids = new HashSet<>();
    private final BigDecimal validRatio;

    public Chain(Node root) {
        this.path.add(root);
        this.ids.add(root.getId());
        this.validRatio = new BigDecimal("1");
    }

    public Chain(Chain oldChain, Edge edge, Node node) {
        // copy
        this.path.addAll(oldChain.getPath());
        this.ids.addAll(oldChain.getIds());
        // add
        this.path.add(edge);
        this.path.add(node);
        this.ids.add(node.getId());
        this.validRatio = oldChain.getValidRatio().multiply(edge.isDottedLine() ? ZERO : edge.getRatio());
    }

    public Node getLast() {
        return (Node) this.path.get(path.size() - 1);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Object obj : path) {
            if (obj instanceof Node) {
                builder.append(String.format("%s(%s)", ((Node) obj).getName(), ((Node) obj).getId()));
            } else if (obj instanceof Edge) {
                builder.append(String.format("-%s%s->", ((Edge) obj).getRatio().stripTrailingZeros().toPlainString(), ((Edge) obj).isDottedLine() ? "(x)" : ""));
            }
        }
        return builder.toString();
    }
}
