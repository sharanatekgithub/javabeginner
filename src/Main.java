import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
//        operateVehicle((flag , s) -> flag ? "Car running at "+s : "Car Stopped at 0", false, 10);
//        operateVehicle((flag , s) -> flag ? "Car running at "+s : "Car Stopped at 0", true, 15);

        List<Integer> lst = Arrays.asList(1, 2, 3, 4, 5);

        for (Integer s : lst) {
            if (s.intValue() % 2 == 0) {
                System.out.println(s);
            }
        }

        List newList = lst.stream()
                .filter(i -> i.intValue() % 2 == 0)
                .collect(Collectors.toList());

        System.out.println(newList);
    }

    public static void operateVehicle(Vehicle v, boolean flag, int s){
        System.out.println(v.drive(flag, s));
    }

    public static boolean isEven(Integer i) {
        return i.intValue() % 2 == 0;
    }
}

interface Vehicle{
    String drive(boolean flag , int s);
}