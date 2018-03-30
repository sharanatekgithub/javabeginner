
public class TestEqualsMethod {

    public static void main(String[] args) {
        DataClass c1 = new DataClass(0);
        DataClass c2 = new DataClass(0);
        System.out.println(c1.equals(c2));
        System.out.println(c2.equals(c1));
    }


}

class DataClass {
    private int value;

    public DataClass(int value){
        this.value = value;
    }
}