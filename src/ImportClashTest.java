import javafxgui.*;

public class ImportClashTest {
    public static void main(String[] args) {
        System.out.println("checking");

        HelloWorldGui guy = new HelloWorldGui();
        guy.print();
        javafxgui.HelloWorldGui guy1 = new javafxgui.HelloWorldGui();
        guy1.main(args);
    }
}

class HelloWorldGui{
    public void print(){
        System.out.println("printing from a local class");
    }
}
