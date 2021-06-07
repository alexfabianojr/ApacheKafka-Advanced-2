package br.com.alura.ecommerce;

import br.com.alura.LocalDatabase;
import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists(createUsersTable());
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        new ServiceRunner(CreateUserService::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing user, checking if is new");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var order = record.value();

//        if(isNewUser(order.getEmail())) {
//            insertNewUser(order.getEmail());
//        }
        insertNewUser(order.getPayload().getEmail());
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String email) throws SQLException {
        var query = new StringBuilder();
        query.append("  INSERT INTO                 ");
        query.append("      USERS   (uuid, email)   ");
        query.append("  VALUES                      ");
        query.append("      (?, ?)                  ");
        var uuid = UUID.randomUUID().toString();
        this.database.update(query.toString(), uuid, email);
        System.out.printf("\nUsuário inserido com sucesso! email: %s, uuid: %s \n", email, uuid);
    }

    private boolean isNewUser(String email) throws SQLException {
        StringBuilder query = new StringBuilder();
        query.append("  SELECT                  ");
        query.append("      u.uuid              ");
        query.append("  FROM                    ");
        query.append("      USERS u             ");
        query.append("  WHERE                   ");
        query.append("      u.email = ? limit 1 ");
        var results = this.database.query(query.toString(), email);
        return !results.first();
    }

    public static final String createUsersTable() {
        StringBuilder query = new StringBuilder();
        query.append("  CREATE TABLE USERS (                ");
        query.append("      uuid varchar(255) primary key,  ");
        query.append("      email varchar(200))             ");
        return query.toString();
    }
}
