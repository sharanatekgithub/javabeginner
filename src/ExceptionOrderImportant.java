public class ExceptionOrderImportant {
    public static void main(String[] args ) {
      try{
            System.out.print("A");
            throw new RuntimeException();
      } catch (Exception e1) {

          System.out.print("E1");
        }
     /*   catch (RuntimeException e2) {

          System.out.print("E2");
       }*/
      finally{
          System.out.print("F");
        }
    }
}
