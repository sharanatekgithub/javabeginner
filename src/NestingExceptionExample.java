public class NestingExceptionExample {

    public static void main(String[] args) throws Exception {
        Object[] localArgs = (Object[]) args;

        try {
            Integer[] numbers = (Integer[]) localArgs;
        } catch (ClassCastException originalException) {
            Exception generalException = new Exception(
                    "Horrible exception!",
                    originalException);
            throw generalException;
        }
    }
}