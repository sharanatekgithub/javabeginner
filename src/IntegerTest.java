
public class IntegerTest {
    public static void main(String[] args) {
        System.out.println("Bytes used to represent an int "+Integer.BYTES);
        System.out.println("Bits used to represent an int  "+Integer.SIZE);
        System.out.println("max value int can have "+Integer.MAX_VALUE);
        System.out.println("min value int can have "+Integer.MIN_VALUE);
        System.out.println("class representing primitive type "+Integer.TYPE);

        Integer i1 = new Integer(5); //supply integer to construct object
        Integer i2 = new Integer("5");//supply string to get Integer obj

        //compare
        if (i1.intValue()==i2.intValue()){
            System.out.println("Both objects have same value ");
        }

        int c = i1.compareTo(i2);
        System.out.println("the comparison result" +c);

        c = Integer.compare(1,2);
        System.out.println("the comparison result" +c);

        c = Integer.compareUnsigned(-5,-10);
        System.out.println("the comparison result" +c);

        //ways to get primtive int and string type to Object type
        Integer i3 = Integer.valueOf(3);
        Integer i4 = Integer.valueOf("4");
        //returns unsigned decimal string
        System.out.println("string rep"+Integer.toUnsignedString(2));
    }

}
