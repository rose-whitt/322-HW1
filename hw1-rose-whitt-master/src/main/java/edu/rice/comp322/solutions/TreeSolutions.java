package edu.rice.comp322.solutions;

import edu.rice.comp322.provided.trees.GList;
import edu.rice.comp322.provided.trees.Tree;

import java.util.function.BiFunction;

public class TreeSolutions {

    // @TODO Implement tree sum

    /**
     * Recursively sums the values of all the nodes in the given Tree of Integers without
     * using higher order functions, mutation, or loops.
     * @param tree  the tree to sum
     * @return  the sum of the values in the given tree
     */
    public static Integer problemOne(Tree<Integer> tree) {
        if (tree.children().isEmpty()) {
            return tree.value();
        } else {
            return tree.value() + problemOne(tree.children().head()) + problemOne(Tree.makeNode(0, tree.children().tail()));
        }
    }



    // @TODO Implement tree sum using higher order list functions
    /**
     * Use the GList functions map, fold, and filter.
     *
     * @param tree: An instance of the Tree class
     * @return: the sum
     */
    public static Integer problemTwo(Tree<Integer> tree) {
        int val = tree.value();
        return val + tree.children().fold(0, (x, valval) -> x + problemTwo(valval));
    }

    /*
     * Problem 3's solution should be written in the Tree.java class at line 118.
     */


    // @TODO Calculate the sum of the elements of the tree using tree fold
    public static Integer problemFour(Tree<Integer> tree) {
        return tree.value() + tree.children().fold(0, (x, y) -> x + problemFour(y));
    }

}
