/*
An example to test clash between imported Type Vs local Type declaration;
its completly valid , but when we refer using a simple name , its referring to Local type
, to refer to the type from imported package fully qualified name needs to be used
 */
import javafxgui.*;

public class ImportClashTest {
    public static void main(String[] args) {
        System.out.println("checking");

        HelloWorldGui guy = new HelloWorldGui();
        guy.print();
     //   javafxgui.HelloWorldGui guy1 = new javafxgui.HelloWorldGui();
     //   guy1.main(args);
    }
}

class HelloWorldGui{
    public void print(){
        System.out.println("printing from a local class");
    }
}
