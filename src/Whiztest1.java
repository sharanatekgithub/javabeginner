import static java.lang.System.*;

public class Whiztest1 {

    public static void main(String[] args){
      A ab = new B();
      System.out.print(ab.j);
    }
}

class A {
    protected  int x =10;
    static int j = 21 ;

}


class B extends A {}
