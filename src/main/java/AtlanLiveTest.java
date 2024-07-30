import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.model.assets.Glossary;

public class AtlanLiveTest {
    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        System.out.println("Hellow Work");
        try {
            Glossary x = Glossary.findByName("jkuchmek_test");
            System.out.println(x);
        } catch (AtlanException e) {
            e.printStackTrace();
        }

    }
}