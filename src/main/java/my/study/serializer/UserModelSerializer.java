package my.study.serializer;

import my.study.model.User;
import my.study.model.Work;
import my.study.model.WorkDomain;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class UserModelSerializer {

    public static final String TARGET_FILE = "users.avro";

    public static void main(String[] args) throws IOException {
        List<User> users = getUsers();
        serializeUsers(users);
    }

    private static void serializeUsers(List<User> users) throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(users.get(0).getSchema(), new File(TARGET_FILE));
        for (User user : users) {
            dataFileWriter.append(user);

        }
        dataFileWriter.close();
    }


    private static List<User> getUsers() {
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);

        User user2 = new User("Ben", 7, "red", new Work("first place", WorkDomain.ECONOMY));

        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
        return Arrays.asList(user1, user2, user3);
    }

}
