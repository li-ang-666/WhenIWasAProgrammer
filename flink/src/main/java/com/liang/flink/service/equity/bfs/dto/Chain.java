package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Data
public class Chain implements Serializable {
    private static final BigDecimal ZERO = new BigDecimal("0");
    private final List<Object> path = new ArrayList<>();
    private final Set<String> ids = new LinkedHashSet<>();
    private final BigDecimal ratio;

    public Chain(Node root) {
        this.path.add(root);
        this.ids.add(root.getId());
        this.ratio = new BigDecimal("1");
    }

    public Chain(Chain oldChain, Edge edge, Node node) {
        // copy
        this.path.addAll(oldChain.getPath());
        this.ids.addAll(oldChain.getIds());
        // add
        this.path.add(edge);
        this.path.add(node);
        this.ids.add(node.getId());
        this.ratio = oldChain.getRatio().multiply(edge.isDottedLine() ? ZERO : edge.getRatio());
    }

    public Node getLast() {
        return (Node) this.path.get(path.size() - 1);
    }
}
