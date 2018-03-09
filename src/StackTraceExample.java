public class StackTraceExample {
    public static void main(String[] args) {
        method1();
    }

    public static void method1() {
        method11();
    }

    public static void method11() {
        method111();
    }

    public static void method111() {
        throw new NullPointerException("Fictitious NullPointerException");
    }
}