class Animal1 {
    public void eat() throws Exception {
        System.out.print("Animal eats");
    }
}
public class OverrideTest extends Animal1{
    public void eat() {
        System.out.print("Dog eats");
    }

    public static void main(String [] args) {
        Animal1 a = new OverrideTest();
        OverrideTest d = new OverrideTest();
        //d.eat();
        // try {
        d.eat();
        //   } catch (Exception e){}


    }
}
