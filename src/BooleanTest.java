public class BooleanTest {
    public static void main(String[] args) {
        Boolean b = true;       //Object implicitly created
        System.out.println(b);

        b= Boolean.logicalAnd(b.booleanValue(), false);
        System.out.println(b);

        //above statement is equivalent to
        b= Boolean.logicalAnd(true, false);
        System.out.println(b);

        b= Boolean.logicalOr(true, false);
        System.out.println("value in b "+b);

        Boolean b1 = new Boolean(false);
        System.out.println("b1="+b1);

        Boolean b2 = new Boolean(false);
        System.out.println("b2="+b2);

        //compare is a static method to compare two boolean values
        System.out.println("compare b(true) value with b1(true)"+Boolean.compare(b.booleanValue() , b1.booleanValue()));

        System.out.println("compare b1(false) value with b(true)"+Boolean.compare(b1.booleanValue() , b.booleanValue()));

        System.out.println("compare (true) value with true"+Boolean.compare(true , true));


        //compareTo  is instance method to compare current Object's value witht the specified argument
        System.out.println("compare b1(true) value with b1(true)"+b1.compareTo( b.booleanValue()));

        Boolean b3 = new Boolean("Sharana");
        System.out.println("b3="+b3);

        Boolean b4 = new Boolean("true");
        System.out.println("b4="+b4);

        b4 = new Boolean("TRUE");
        System.out.println("b4="+b4);

        Boolean b5 = Boolean.valueOf(true);
        System.out.println("b5= "+b5);

        Boolean b6 = Boolean.valueOf("false");
        System.out.println("b6= "+b6);
    }
}
