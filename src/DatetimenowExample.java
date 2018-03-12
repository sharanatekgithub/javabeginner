import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.time.LocalDateTime.*;

public class DatetimenowExample {
    public static void main(String[] args ){
        LocalDateTime ldt = LocalDateTime.now();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY/MM/dd hh:mm:ss");

        System.out.println(dtf.format(ldt));
    }
}
