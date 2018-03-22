
public class StringReplaceTest {
        public static void main(String[] args) {
            StringBuffer sb = new StringBuffer("012345");
            //The substring begins at the specified start and extends to the character at index end - 1
            // or to the end of the sequence if no such character exists.
//            System.out.println(sb.replace(2,3,"P"));
            System.out.println(sb.replace(4,3,"P"));
         }
    }

