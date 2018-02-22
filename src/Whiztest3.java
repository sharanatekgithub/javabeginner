public class Whiztest3 {
    public static void main(String[] args){
        int i, j;
        whizlabs: for(i = 1; i < 4 ; i++ ){
            j = 1 ;
            while ( j < 4 ) {
                if ( j%2 == 0){
                    break ;
                }
                j++ ;
                System.out.print("i="+i+", j= "+j+"inner");
            }
            System.out.print("i="+i+", j= "+j+"outer");
        } //end of wizlabs

    }
}
