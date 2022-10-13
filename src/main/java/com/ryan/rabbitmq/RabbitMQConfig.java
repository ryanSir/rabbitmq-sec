package com.ryan.rabbitmq;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author ryan
 * @version Id: RabbitMQConfig, v 0.1 2022/10/12 3:52 PM ryan Exp $
 */
@Configuration
@ComponentScan({"com.ryan.rabbitmq.*"})
public class RabbitMQConfig {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses("20.50.54.194:5672");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setVirtualHost("my_rabbit");
        return connectionFactory;
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }

    /**
     * 针对消费者配置
     * 1. 设置交换机类型
     * 2. 将队列绑定到交换机
     * 一个交换机可以绑定不同的队列，使用不同的路由key来操作
     * FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
     * HeadersExchange ：通过添加属性key-value匹配
     * DirectExchange: 按照routing key分发到指定队列
     * TopicExchange:多关键字匹配
     *
     * @return
     */
    @Bean
    public TopicExchange exchange001() {
        return new TopicExchange("topic001", true, false);
    }

    @Bean
    public Queue queue001() {
        return new Queue("queue001", true);
    }

    @Bean
    public Queue queue002() {
        return new Queue("queue002", true);
    }

    @Bean
    public Binding binding001() {
        return BindingBuilder.bind(queue001()).to(exchange001()).with("spring.#");
    }

    @Bean
    public Binding binding002() {
        return BindingBuilder.bind(queue002()).to(exchange001()).with("mq.#");
    }

    @Bean
    public Queue queue_image() {
        return new Queue("image_queue", true);
    }

    @Bean
    public Queue queue_pdf() {
        return new Queue("pdf_queue", true);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        return rabbitTemplate;
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        // 监听多个队列
        container.setQueues(queue001(), queue002(), queue_image(), queue_pdf());
        // 设置当前消费者的数量
        container.setConcurrentConsumers(1);
        // 设置最大消费者数量
        container.setMaxConcurrentConsumers(5);
        // 重回队列,不设置重回队列
        container.setDefaultRequeueRejected(false);
        // ack签收模式,auto:自动签收
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        // 消费端的标签策略
        container.setConsumerTagStrategy(new ConsumerTagStrategy() {
            @Override
            public String createConsumerTag(String queue) {
                return queue + "_" + UUID.randomUUID().toString();
            }
        });
        // 1. 默认
//        container.setMessageListener(new ChannelAwareMessageListener() {
//            @Override
//            public void onMessage(Message message, Channel channel) throws Exception {
//                String msg = new String(message.getBody());
//                System.out.println("------ 消费者 ：" + msg);
//            }
//        });

//        // 2. 适配器
//        MessageListenerAdapter adapter = new MessageListenerAdapter(new MessageDelegate());
//        adapter.setDefaultListenerMethod("consumeMessage");
//        // 消息类型转换器
//        adapter.setMessageConverter(new TextMessageConverter());
//        container.setMessageListener(adapter);
//

//         // 3. 队列指定投递到具体方法
//        MessageListenerAdapter adapter = new MessageListenerAdapter(new MessageDelegate());
//        adapter.setMessageConverter(new TextMessageConverter());
//        Map<String, String> queueOrTagToMethodName = new HashMap<>();
//        queueOrTagToMethodName.put("queue001", "method1");
//        queueOrTagToMethodName.put("queue002", "method2");
//        adapter.setQueueOrTagToMethodName(queueOrTagToMethodName);
//
//        container.setMessageListener(adapter);


        return container;
    }


}
