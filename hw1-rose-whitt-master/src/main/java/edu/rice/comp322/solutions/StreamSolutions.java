package edu.rice.comp322.solutions;

import edu.rice.comp322.provided.streams.models.Customer;
import edu.rice.comp322.provided.streams.models.Order;
import edu.rice.comp322.provided.streams.models.Product;
import edu.rice.comp322.provided.streams.provided.DiscountObject;
import edu.rice.comp322.provided.streams.repos.CustomerRepo;
import edu.rice.comp322.provided.streams.repos.OrderRepo;
import edu.rice.comp322.provided.streams.repos.ProductRepo;

import java.util.*;

import java.time.LocalDate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class containing all of the students implemented solutions to the stream problems.
 */
public class StreamSolutions {

    /**
     * Use this function with the appropriate streams solution test to ensure repos load correctly.
     */
    public static List<Long> problemZeroSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        List<Long> counts = new ArrayList<>();
        counts.add(customerRepo.findAll().stream().count());
        counts.add(orderRepo.findAll().stream().count());
        counts.add(productRepo.findAll().stream().count());

        return counts;
    }

    /**
     * Use this function with the appropriate streams solution test to ensure repos load correctly.
     */
    public static List<Long> problemZeroPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        List<Long> counts = new ArrayList<>();
        counts.add(customerRepo.findAll().parallelStream().count());
        counts.add(orderRepo.findAll().parallelStream().count());
        counts.add(productRepo.findAll().parallelStream().count());

        return counts;
    }

    // TODO: Implement problem one using sequential streams

    /**
     * Sequentially calculates the companies maximum possible revenue from all online sales
     *      during the month of February.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Double representing the calculated sum given the above conditions
     */
    public static Double problemOneSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Stream<Double> sums = orderRepo.findAll().stream()
                // filter out non-april
                .filter(order -> order.getOrderDate().getMonthValue() == 2)
                // get total price for each order
                // for each order
                .map(order ->
                        order.getProducts().stream()
                                .map(Product::getFullPrice)
                                .reduce(0.0, Double::sum)
                );
        var total = sums.reduce(0.0, Double::sum);
        return total;
    }

    /**
     * Calculates the companies maximum possible revenue from all online sales
     *      during the month of February in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Double representing the calculated sum given the above conditions
     */
    public static Double problemOnePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Stream<Double> sums = orderRepo.findAll().stream().parallel()
                // filter out non-april
                .filter(order -> order.getOrderDate().getMonthValue() == 2)
                // get total price for each order
                // for each order
                .map(order ->
                        order.getProducts().stream().parallel()
                                .map(Product::getFullPrice)
                                .reduce(0.0, Double::sum)
                );
        var total = sums.reduce(0.0, Double::sum);
        return total;
    }

    /**
     * Sequentially get the Order IDs of the 5 most recently placed orders.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Set of type Long contained the Order IDs of the 5 most recently placed orders
     */
    public static Set<Long> problemTwoSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Set<Long> sorted = orderRepo.findAll().stream()
                .sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .map(order -> order.getId())
                .limit(5)
                .collect(Collectors.toSet());
        return sorted;
    }

    /**
     * Get the Order IDs of the 5 most recently placed orders in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Set of type Long contained the Order IDs of the 5 most recently placed orders
     */
    public static Set<Long> problemTwoPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Set<Long> sorted = orderRepo.findAll().stream().parallel()
                .sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .map(order -> order.getId())
                .limit(5)
                .collect(Collectors.toSet());
        return sorted;
    }

    // TODO: Implement problem three using sequential streams

    /**
     * Count the number of distinct customers making purchases.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Long representing the number of distinct customers who have made a purchase
     */
    public static Long problemThreeSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var distinct_count = orderRepo.findAll().stream()
                        .map(order -> order.getCustomer()).distinct().count();

        return distinct_count;
    }

    /**
     * Count the number of distinct customers making purchases.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Long representing the number of distinct customers who have made a purchase
     */
    public static Long problemThreePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var distinct_count = orderRepo.findAll().stream().parallel()
                .map(order -> order.getCustomer()).distinct().count();

        return distinct_count;
    }

    // TODO: Implement problem four using sequential streams

    /**
     * Sequentially calculates the total discount for all orders placed in March 2021.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Double representing the total discount with the above conditions
     */
    public static Double problemFourSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        // get order stream
            // filter to only march 2021
        var sums = orderRepo.findAll().stream()
                // keep only orders in March 2021
                .filter(order -> (order.getOrderDate().getMonthValue() == 3 && order.getOrderDate().getYear() == 2021))
                // get total price for each order
                // for each order
                .map(order ->
                        order.getProducts().stream()
                                .map(product -> new DiscountObject(order.getCustomer(), product).getDiscount())
                                .reduce(0.0, Double::sum)
                )
                .reduce(0.0, Double::sum);
        return sums;
    }

    /**
     * Calculates the total discount for all orders placed in March 2021 in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a value of type Double representing the total discount with the above conditions
     */
    public static Double problemFourPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var sums = orderRepo.findAll().stream().parallel()
                // keep only orders in March 2021
                .filter(order -> (order.getOrderDate().getMonthValue() == 3 && order.getOrderDate().getYear() == 2021))
                // get total price for each order
                // for each order
                .map(order ->
                        order.getProducts().stream().parallel()
                                .map(product -> new DiscountObject(order.getCustomer(), product).getDiscount())
                                .reduce(0.0, Double::sum)
                )
                .reduce(0.0, Double::sum);
        return sums;
    }

    /**
     * Sequentially creates a mapping between customers IDs and the total dollar
     * amount they spent on products.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Map of key type Long and value type Double representing customerIDs and total dollar amount spent
     */
    public static Map<Long, Double> problemFiveSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var result = orderRepo.findAll().stream()
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.summingDouble(order -> order.getProducts().stream()
                                .map(prod -> prod.getFullPrice())
                                .reduce(0.0, Double::sum))));
        return result;
        //var t3 = t2.map();
    }

    /**
     * Creates a mapping between customers IDs and the total dollar in parallel
     * amount they spent on products in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Map of key type Long and value type Double representing customerIDs and total dollar amount spent
     */
    public static Map<Long, Double> problemFivePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var result = orderRepo.findAll().stream().parallel()
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.summingDouble(order -> order.getProducts().stream().parallel()
                                .map(prod -> prod.getFullPrice())
                                .reduce(0.0, Double::sum))));
        return result;
    }


    /**
     * Sequentially creates a mapping between product categories and the average cost of an item in that category.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Map of key type String and value type Double representing product categories
     *              and average cost of an item in the key category
     */
    public static Map<String, Double> problemSixSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var grouping_count = productRepo.findAll().stream()
                .collect(Collectors.groupingBy(prod -> prod.getCategory(), Collectors.averagingDouble(prod -> prod.getFullPrice())));
        return grouping_count;
    }

    /**
     * Creates a mapping between product categories and the average cost of an item in that category in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return a Map of key type String and value type Double representing product categories
     *              and average cost of an item in the key category
     */
    public static Map<String, Double> problemSixPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var grouping_count = productRepo.findAll().stream().parallel()
                .collect(Collectors.groupingBy(prod -> prod.getCategory(), Collectors.averagingDouble(prod -> prod.getFullPrice())));
        return grouping_count;
    }


    /**
     * Sequentially creates a mapping between products IDs in the tech category (category = ”Tech”) and the IDs of the
     * customers which ordered them.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return a Map of key type Long and value type Set of type Long representing product IDs in tech category
     *              and IDs of customers who ordered them
     */
    public static Map<Long, Set<Long>> problemSevenSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var tech = productRepo.findAll().stream()
                .filter(prod -> prod.getCategory().equals("Tech"))
                .collect(Collectors.groupingBy(prod -> prod.getId(),
                        Collectors.flatMapping(prods -> prods.getOrders().stream()
                                        .map(order -> order.getCustomer().getId()), Collectors.toSet()
                        )));
        return tech;
    }

    /**
     * Creates a mapping between products IDs in the tech category (category = ”Tech”) and the IDs of the
     * customers which ordered them in parallel.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return a Map of key type Long and value type Set of type Long representing product IDs in tech category
     *              and IDs of customers who ordered them
     */
    public static Map<Long, Set<Long>> problemSevenPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var tech = productRepo.findAll().stream().parallel()
                .filter(prod -> prod.getCategory().equals("Tech"))
                .collect(Collectors.groupingBy(prod -> prod.getId(),
                        Collectors.flatMapping(prods -> prods.getOrders().stream().parallel()
                                .map(order -> order.getCustomer().getId()), Collectors.toSet()
                        )));
        return tech;
    }

    /**
     * Sequentially creates a mapping between the IDs of customers without membership tiers and their sales utilization
     * rate.
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Map of key type Long and value type Double of type representing customer IDs and their sales
     *              utilization rate.
     */
    public static Map<Long, Double> problemEightSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var result = orderRepo.findAll().stream()
                .filter(order -> order.getCustomer().getTier() == 0)
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.flatMapping(order -> order.getProducts().stream()
                                        .map(product -> new DiscountObject(order.getCustomer(), product))
                                        .map(disc -> disc.getDiscount() == 0.0 ? 0.0 : 1.0), Collectors.averagingDouble(num -> num))));
        return result;
    }

    /**
     * Creates a mapping between the IDs of customers without membership tiers and their sales utilization
     * rate in parallel.
     *
     * @param customerRepo  a Java collection of Customer information
     * @param orderRepo     a Java collection of Order information
     * @param productRepo   a Java collection of Product information
     * @return  a Map of key type Long and value type Double of type representing customer IDs and their sales
     *              utilization rate.
     */
    public static Map<Long, Double> problemEightPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var result = orderRepo.findAll().stream()
                .filter(order -> order.getCustomer().getTier() == 0)
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.flatMapping(order -> order.getProducts().stream()
                                .map(product -> new DiscountObject(order.getCustomer(), product))
                                .map(disc -> disc.getDiscount() == 0.0 ? 0.0 : 1.0), Collectors.averagingDouble(num -> num))));
        return result;
    }
}
