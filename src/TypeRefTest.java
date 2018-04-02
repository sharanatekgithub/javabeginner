
interface I {
    void meth();
}

class A1 implements I {
    void A1(String s) {  }
    public void meth() { System.out.print("A1");  }
}
class C1 extends A1 implements I {
    public void meth() {
        System.out.print("C1");
    }
}

public class TypeRefTest {
    public static void main(String args [] ) {
//        A1 a = new A1();
//        C1 cl = (C1)a;
//        cl.meth();

        // if A1 was Holding reference to Child class C1
        //then casting would have been successful
        A1 a = new C1();
        C1 cl = (C1)a;
        cl.meth();
    }
}

