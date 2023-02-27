package com.liang.study.structure;

import com.sun.istack.internal.NotNull;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;

public class BinaryTree {
    private TreeNode root;

    /**
     * 添加元素
     * 相等时不重复添加
     */
    public void add(int i) {
        //涉及到引用传递的知识, root不能在下面的add方法里初始化, 只能在这里初始化
        if (root == null) {
            root = new TreeNode(i);
        } else {
            add(i, root);
        }
    }

    private void add(int i, TreeNode node) {
        if (i < node.getValue()) {
            if (node.getLeft() == null) {
                node.setLeft(new TreeNode(i));
            } else {
                add(i, node.getLeft());
            }
        } else if (i > node.getValue()) {
            if (node.getRight() == null) {
                node.setRight(new TreeNode(i));
            } else {
                add(i, node.getRight());
            }
        }
    }

    /**
     * 深度优先遍历:
     * 前序遍历(根左右)
     * 中序遍历(左根右)
     * 后序遍历(左右根)
     */
    public void depthFirstSearch() {
        depthFirstSearch(root);
    }

    private void depthFirstSearch(TreeNode node) {
        if (node != null) {
            System.out.println(node.getValue());
            depthFirstSearch(node.getLeft());
            depthFirstSearch(node.getRight());
        }
    }

    /**
     * 广度优先遍历:
     * 层序遍历
     */
    public void breadthFirstSearch() {
        if (root != null) {
            LinkedList<TreeNode> queue = new LinkedList<>();
            //放入根节点
            queue.addLast(root);
            while (!queue.isEmpty()) {
                //循环: 取出父节点, 操作value(打印), 操作left(放入), 操作right(放入)
                TreeNode node = queue.removeFirst();
                System.out.println(node.getValue());
                if (node.getLeft() != null) {
                    queue.addLast(node.getLeft());
                }
                if (node.getRight() != null) {
                    queue.addLast(node.getRight());
                }
            }
        }
    }
}

@Data
@RequiredArgsConstructor
class TreeNode {

    /**
     * 任意TreeNode对象, value不为空
     */
    private @NotNull Integer value;
    private TreeNode left;
    private TreeNode right;
}
