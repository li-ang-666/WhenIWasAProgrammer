package com.liang.study.structure;

import com.sun.istack.internal.NotNull;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.junit.Test;

public class List {
    @Test
    public void test() {
        List list = new List();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.delete(1);
        list.delete(1);
        list.printAll();
    }

    private Node root;
    private int length = 0;

    /**
     * 添加元素
     */
    public void add(int i) {
        //涉及到引用传递的知识, root不能在下面的add方法里初始化, 只能在这里初始化
        if (root == null) {
            root = new Node(i);
            length++;
        } else {
            add(i, root);
        }
    }

    private void add(int i, Node node) {
        if (node.getNext() == null) {
            node.setNext(new Node(i));
            length++;
        } else {
            add(i, node.getNext());
        }
    }

    /**
     * 遍历
     */
    public void printAll() {
        printAll(root);
    }

    private void printAll(Node node) {
        if (node != null) {
            System.out.println(node.getValue());
            printAll(node.getNext());
        }
    }

    /**
     * 删除元素
     */
    public void delete(int index) {
        //首先埋下一个成员变量, 插入的时候自增1, 删除的时候自减1, 用于判断入参是否越界
        if (index < 0 || index > length - 1) {
            System.out.println("越界, 删除失败");
        } else {
            if (index == 0) {
                root = root.getNext();
            } else {
                Node current = root;
                for (int i = index; i > 1; i--) {
                    current = current.getNext();
                }
                current.setNext(current.getNext().getNext());
            }
            length--;
        }
    }
}

@Data
@RequiredArgsConstructor
class Node {
    private @NotNull Integer value;
    private Node next;
}
