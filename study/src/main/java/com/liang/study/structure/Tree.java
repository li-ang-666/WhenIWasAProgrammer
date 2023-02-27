package com.liang.study.structure;

import org.junit.Test;

import java.util.LinkedList;

public class Tree {
    @Test
    /**
     *                    7
     *            |               |
     *            4               9
     *        |       |       |       |
     *        2       6       8       10
     *      |   |
     *      1   3
     */
    public void test() {
        TreeNode tree = new TreeNode();
        tree.add(7);
        tree.add(4);
        tree.add(9);
        tree.add(2);
        tree.add(6);
        tree.add(8);
        tree.add(10);
        tree.add(1);
        tree.add(3);

        System.out.println("深度优先:");
        tree.depthFirsSearch();
        System.out.println("广度优先:");
        tree.breadthFirstSearch();
    }
}

class TreeNode {
    private Integer value;
    private TreeNode left;
    private TreeNode right;

    //无参构造, 用于外界初始化, 仅在第一次add的时候懒加载
    public TreeNode() {
    }

    //有参构造, 用于内部每次添加节点, value必不为空
    private TreeNode(int value) {
        this.value = value;
    }

    /**
     * 添加数据, 相等时不重复添加
     */
    public void add(int i) {
        if (value == null) {
            value = i;
        } else if (i < value) {
            if (left == null) {
                left = new TreeNode(i);
            } else {
                left.add(i);
            }
        } else if (i > value) {
            if (right == null) {
                right = new TreeNode(i);
            } else {
                right.add(i);
            }
        }
    }

    /**
     * 深度优先遍历:
     * 前序遍历(根左右)
     * 中序遍历(左根右)
     * 后序遍历(左右根)
     */
    public void depthFirsSearch() {
        //未经过懒加载, 说明没数, 不用遍历
        if (value == null) {
            return;
        }
        System.out.println(value);
        if (left != null) {
            left.depthFirsSearch();
        }
        if (right != null) {
            right.depthFirsSearch();
        }
    }

    /**
     * 广度优先遍历:
     * 层序遍历
     */
    public void breadthFirstSearch() {
        //未经过懒加载, 说明没数, 不用遍历
        if (value == null) {
            return;
        }
        LinkedList<TreeNode> queue = new LinkedList<>();
        //放入根节点
        queue.addLast(this);
        //循环: 取出父节点, 打印value, 放入left, 放入right
        while (!queue.isEmpty()) {
            TreeNode node = queue.removeFirst();
            System.out.println(node.value);
            if (node.left != null) {
                queue.addLast(node.left);
            }
            if (node.right != null) {
                queue.addLast(node.right);
            }
        }
    }
}
