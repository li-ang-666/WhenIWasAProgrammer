package com.liang.study.structure;

import lombok.Getter;
import lombok.Setter;
import org.junit.Test;

public class BinaryTree {
    @Test
    public void test() {
        Tree tree = new Tree();
        tree.add(2);
        tree.add(1);
        tree.add(3);
        tree.preOrderPrint();
    }
}

class Tree {
    private TreeNode root;

    public void add(int i) {
        add(i, root);
    }

    private void add(int i, TreeNode node) {
        if (node == null) {
            node = new TreeNode(i);
            System.out.println(node);
        } else if (i < node.getValue()) {
            add(i, node.getLeft());
        } else if (i > node.getValue()) {
            add(i, node.getRight());
        }
    }

    public void preOrderPrint() {
        preOrderPrint(root);
    }

    private void preOrderPrint(TreeNode node) {
        if (node != null) {
            System.out.println(node.getValue());
            preOrderPrint(node.getLeft());
            preOrderPrint(node.getRight());
        }
    }
}

@Getter
@Setter
class TreeNode {
    private int value;
    private TreeNode left;
    private TreeNode right;

    public TreeNode(int value) {
        this.value = value;
    }
}



