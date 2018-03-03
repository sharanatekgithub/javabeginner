import java.util.ArrayList;
import java.util.List;

class Data {
    int value;
    Data(int value){
        this.value = value;
    }
}
public class ObjRefpass {


 private  void swapData(Data data1 , Data data2){
    Data temp = data1;
       //System.out.println("temp.value = "+ temp.value + " data2.value "+ data2.value);
    data1 = data2;
       //System.out.println("data1.value = "+ data1.value + " data2.value "+ data2.value);
    data2 = temp;
      // System.out.println("data1.value = "+ data1.value + " data2.value "+ data2.value);
}

private ArrayList<Data> swapobj(Data data1 , Data data2){
        Data temp = data1;
        //System.out.println("temp.value = "+ temp.value + " data2.value "+ data2.value);
        data1 = data2;
        //System.out.println("data1.value = "+ data1.value + " data2.value "+ data2.value);
        data2 = temp;
        // System.out.println("data1.value = "+ data1.value + " data2.value "+ data2.value);

        ArrayList<Data> al;
    al = new ArrayList<>();
    al.add(data1);
        al.add(data2);

        return al;
    }
public static void main(String[] args) {
    Data data1 = new Data(-1);
    Data data2 = new Data(1);
    ObjRefpass orp = new ObjRefpass();

    System.out.println("before swap==> "+ data1.value + " "+ data2.value);
    orp.swapData(data1 , data2);
    System.out.println(data1.value + " "+ data2.value);

    ArrayList<Data> al1 = orp.swapobj(data1 , data2);

    // Printing elements one by one
    for (int i=0; i< al1.size(); i++)
        System.out.println("al1."+i+"==>"+al1.get(i).value+" ");
}
}
