public class ProtectedTest {
    public static void main(String[] args) {
      BB bb =  new BB();
        System.out.println("static variable through Extended class"+BB.j);
        System.out.println("access protected variable thru extended class"+bb.i);
        System.out.println(Character.valueOf('C'));
    }
}

class AA {
    protected int i = 0;
    static int j = 1;
}

class BB extends AA {

}
