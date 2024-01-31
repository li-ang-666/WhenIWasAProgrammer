package com.liang.flink.service.equity.bfs.dto.pojo;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.DOWN;

@Data
public class Path implements Serializable {
    private List<PathElement> elements = new ArrayList<>();
    private Set<String> nodeIds = new HashSet<>();
    private BigDecimal validRatio = ONE;

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
        path.validRatio = oldPath.getValidRatio().multiply(edge.isValid() ? ZERO : edge.getRatio());
        return path;
    }

    public Node getLast() {
        return (Node) elements.get(elements.size() - 1);
    }

    public String toDebugString() {
        StringBuilder builder = new StringBuilder(String.format("[%s]", validRatio.setScale(12, DOWN).toPlainString()));
        for (PathElement element : elements) {
            if (element instanceof Node) {
                builder.append(String.format("%s(%s)", ((Node) element).getName(), ((Node) element).getId()));
            } else if (element instanceof Edge) {
                builder.append(String.format("-%s%s->", ((Edge) element).getRatio().setScale(12, DOWN).stripTrailingZeros().toPlainString(), ((Edge) element).isValid() ? "(x)" : ""));
            }
        }
        return builder.toString();
    }
}
