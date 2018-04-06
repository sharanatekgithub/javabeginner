abstract class Animal{
    void run(){
     System.out.print("Animal run");
    }
    abstract void sound();
}
class Dog extends Animal{

        void sound(){
        System.out.print("Bark");
        }

        public void run(){
        System.out.println(" Dog runs");
        }
}

public class ObjectrefTest1 {
        public static void main(String[] args){
         //Casting not required here
         //parent ref type and Actual child Object
         //all functions referenced must have been atleast declared in the class
            Animal dog = new Dog();
            dog.sound();
            dog.run();

            Dog dog1 = new Dog(); // Not possible , cuz Animal is abstract class
            dog1.sound();
            dog1.run();

        }

    }

