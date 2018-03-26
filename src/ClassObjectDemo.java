public class ClassObjectDemo {

    public static void main(String[] args) {

        try {
            // returns the Class object for the class with the specified name
            Class cls = Class.forName("ParametersTest");
            ParametersTest pt = null;
            try {
                pt = (ParametersTest) Class.forName("ParametersTest").newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            pt.swap(new Integer(10), new Integer(20));

            // returns the name and package of the class
            System.out.println("Class found = " + cls.getName());
            System.out.println("Package = " + cls.getPackage());
        } catch(ClassNotFoundException ex) {
            System.out.println(ex.toString());
        }
    }
}
