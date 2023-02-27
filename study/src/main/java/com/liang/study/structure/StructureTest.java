package com.liang.study.structure;

import org.junit.Test;

public class StructureTest {
    /*
     *                    7
     *            |               |
     *            4               9
     *        |       |       |       |
     *        2       6       8       10
     *      |   |
     *      1   3
     */
    @Test
    public void binaryTest() {
        BinaryTree tree = new BinaryTree();
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
        tree.depthFirstSearch();
        System.out.println("广度优先:");
        tree.breadthFirstSearch();
    }

    @Test
    public void ListTest() {
        List list = new List();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.delete(4);
        System.out.println(list);
    }
}
