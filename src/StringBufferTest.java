public class StringBufferTest {

    public static void main(String[] args) {
        Object o = null;
        StringBuffer b = new StringBuffer();
         b.append(o);
        System.out.println(b);
        //string test
        StringBuffer b1 = new StringBuffer();
        System.out.println(b1.append(new String()));

        //test char sequence
        CharSequence cs = "Sharana kampli";
        b1.append(cs);
        System.out.println(b1);
        System.out.println(b1.append(cs , 8, 14));

        //test char array
        char[] chararray = {'S','h','a','r','a','n','a', 'k','a','m','p','l','i'};

        StringBuffer b2 = new StringBuffer();
        //System.out.println(b2.append(chararray));

        //fro position 7 for length of 6 , which is to position 13
        System.out.println(b2.append(chararray , 7, 6));

    }
}
