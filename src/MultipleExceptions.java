public class MultipleExceptions {
    public static void main(String[] args){
        try {
            // Suppose the code here throws any exceptions,
            // then each is handled in a separate catch block.

            int[] tooSmallArray = new int[2];
            int outOfBoundsIndex = 10000;
            tooSmallArray[outOfBoundsIndex] = 1;

            System.out.println("No exception thrown.");
        } catch(NullPointerException ex) {
            System.out.println("Exception handling code for the NullPointerException.");
        } catch(NumberFormatException ex) {
            System.out.println("Exception handling code for the NumberFormatException.");
        } catch(ArithmeticException | IndexOutOfBoundsException ex) {
            System.out.println("Exception handling code for ArithmeticException"
                    + " or IndexOutOfBoundsException.");
        } catch(Exception ex) {
            System.out.println("Exception handling code for any other Exception.");
        }
    }
}
