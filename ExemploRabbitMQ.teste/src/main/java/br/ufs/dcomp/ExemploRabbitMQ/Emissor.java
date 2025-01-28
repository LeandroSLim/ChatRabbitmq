package br.ufs.dcomp.ExemploRabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Emissor {

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("35.171.23.171"); // Alterar
        factory.setUsername("admin");    // Alterar
        factory.setPassword("password"); // Alterar
        factory.setVirtualHost("/");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        System.out.print("User: ");
        Scanner scanner = new Scanner(System.in);
        String user = scanner.nextLine(); 

      
        channel.queueDeclare(user, false, false, false, null);

        
        Thread receptorThread = new Thread(() -> {
            try {
               
                channel.basicConsume(user, true, (consumerTag, delivery) -> {
                    try {
                       
                        byte[] body = delivery.getBody();
                        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(body));
                        List<Object> messageData = (List<Object>) ois.readObject();

                       
                        String data = (String) messageData.get(0);
                        String hora = (String) messageData.get(1);
                        String remetente = (String) messageData.get(2);
                        String mensagem = (String) messageData.get(3);

                       
                        System.out.println(String.format("(%s às %s) %s diz: %s", data, hora, remetente, mensagem));
                        System.out.print(user + ">> "); 
                    } catch (ClassNotFoundException e) {
                        System.out.println("Erro: A classe não foi encontrada ao desempacotar a mensagem.");
                    } catch (Exception e) {
                        System.out.println("Erro ao processar a mensagem: " + e.getMessage());
                    }
                }, consumerTag -> {});
            } catch (Exception e) {
                System.out.println("Erro na thread do receptor: " + e.getMessage());
            }
        });

        receptorThread.start(); 

       
        String message;
        String messageReceptor = "";

        while (true) {
            System.out.print(messageReceptor + ">> ");
            message = scanner.nextLine();

            if (message.equals("exit")) {
                break;
            }

            if (message.startsWith("@")) { 
                messageReceptor = message.substring(1);
                channel.queueDeclare(messageReceptor, false, false, false, null);
                continue;
            }

           

           
            String data = new SimpleDateFormat("dd/MM/yyyy").format(new Date());
            String hora = new SimpleDateFormat("HH:mm").format(new Date());

            
            List<Object> messageData = new ArrayList<>();
            messageData.add(data);   
            messageData.add(hora);   
            messageData.add(user);   
            messageData.add(message); 

          
            byte[] serializedMessage = serialize(messageData);

            
            channel.basicPublish("", messageReceptor, null, serializedMessage);
        }

      
        channel.close();
        connection.close();
        System.out.println("Conexão encerrada.");
    }

   
    public static byte[] serialize(Object obj) throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            return bos.toByteArray();
        }
    }
}
