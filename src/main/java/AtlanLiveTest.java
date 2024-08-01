import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.model.assets.Asset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlanLiveTest {
    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    private static final Logger logger = LoggerFactory.getLogger(AtlanLiveTest.class);

    public static void main(String[] args) {
        logger.info("Start Live test application !!! ");
        try {
            Asset x = Asset.get(Atlan.getDefaultClient(),"4eeab745-c8d0-4910-ae00-480bda083daa",false);
            logger.info(x.getQualifiedName());
        } catch (AtlanException e) {
            e.printStackTrace();
        }

    }
}