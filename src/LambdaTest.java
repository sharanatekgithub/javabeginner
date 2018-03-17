public class LambdaTest {
    public static void main(String[] args){
        printIT((int a, boolean y) -> y ? "ABC" : "xyz");
    }

    public static void printIT(IT i){
        System.out.println(i.print(1, false));
    }
}

interface IT {
    String print(int a, boolean y);
}