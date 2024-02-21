package com.liang.flink.service.equity.bfs.dto.pojo;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class Path implements Serializable {
    private List<PathElement> elements = new ArrayList<>();
    private Set<String> nodeIds = new HashSet<>();
    private BigDecimal validRatio = BigDecimal.ONE;

    private Path() {
    }

    public static Path newPath(Node root) {
        Path path = new Path();
        // elements
        path.elements.add(root);
        // NodeIds
        path.nodeIds.add(root.getId());
        return path;
    }

    public static Path newPath(Path oldPath, Edge edge, Node node) {
        Path path = new Path();
        // elements
        path.elements.addAll(oldPath.elements);
        path.elements.add(edge);
        path.elements.add(node);
        // NodeIds
        path.nodeIds.addAll(oldPath.nodeIds);
        path.nodeIds.add(node.getId());
        path.validRatio = oldPath.getValidRatio().multiply(edge.isValid() ? edge.getRatio() : BigDecimal.ZERO);
        return path;
    }

    public Node getLast() {
        return (Node) elements.get(elements.size() - 1);
    }
}
