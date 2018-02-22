import com.sun.deploy.config.Config;

/**
 * Created by SXK0W7W on 8/26/2017.
 */
public class FirstArray {
    static int x = 5;
    public static void main(String[] args) {
       int intArray[] =new  int[5];
        intArray[0] =  0;
        intArray[1] =  1;
        intArray[2] =  2;
        intArray[3] =  3;
        intArray[4] =  4;

        System.out.println( intArray[0] );
        System.out.println( intArray[1] );
        System.out.println( intArray[2] );
        System.out.println( intArray[3] );
        System.out.println( intArray[4] );

        System.out.println("Array Length"+ intArray.length );

//        for(int i=0; i< intArray.length; i++) {
//            System.out.println("intArray[" + i + "] = " + intArray[i]);
//        }


        //--- improved for loop
        for (int value:intArray){
            System.out.println("improved"+value+" = "+intArray[value]);
        }
    }


}