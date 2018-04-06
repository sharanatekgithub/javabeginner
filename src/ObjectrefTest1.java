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
        System.out.print(" Dog runs");
        }
}

public class ObjectrefTest1 {
        public static void main(String[] args){
            Animal dog = new Dog();
            dog.sound();
            dog.run();
        }
    }

