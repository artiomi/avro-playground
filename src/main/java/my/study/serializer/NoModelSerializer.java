package my.study.serializer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.RECORD;

public class NoModelSerializer {

    public static final String TARGET_FILE = "users_no_model.avro";

    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src\\main\\avro\\user.avsc"));
        List<GenericRecord> records = getRecords(schema);
        serialize(records, schema, TARGET_FILE);
        deserialize(schema, TARGET_FILE);

    }

    private static void deserialize(Schema schema, String serializationFile) throws IOException {

        System.out.println("start deserialization");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(Paths.get(serializationFile).toFile(), datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next();
            System.out.println(user);

        }

    }


    private static void serialize(List<GenericRecord> records, Schema schema, String outFile) throws IOException {
        File file = new File(outFile);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)) {
            dataFileWriter.create(schema, file);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
        }
        System.out.println("serialization completed");
    }

    private static List<GenericRecord> getRecords(Schema schema) {
        GenericData.EnumSymbol workDomain = null;
        GenericData.Record work = null;
        GenericRecord user1 = null;
        GenericRecord user2 = null;
        for (Schema item : schema.getTypes()) {
            if (ENUM.equals(item.getType())) {
                workDomain = new GenericData.EnumSymbol(item, "IT");
            } else if (RECORD.equals(item.getType())) {
                if ("Work".equals(item.getName())) {
                    work = new GenericData.Record(item);
                    work.put("name", "first place");
                    work.put("domain", workDomain);
                } else if (item.getAliases().contains("my.study.model.Person")) {
                    user1 = new GenericData.Record(item);
                    user1.put("name", "Alyssa");
                    user1.put("favorite_number", 256);
                    user1.put("placeOfWork", work);

                    user2 = new GenericData.Record(item);
                    user2.put("name", "Ben");
                    user2.put("favorite_number", 7);
                    user2.put("favorite_color", "red");
                    user2.put("placeOfWork", work);
                }
            }
        }


        return Arrays.asList(user1, user2);
    }

}
