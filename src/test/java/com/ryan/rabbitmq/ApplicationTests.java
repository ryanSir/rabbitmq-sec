package com.ryan.rabbitmq;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @author ryan
 * @version Id: ApplicationTests, v 0.1 2022/10/12 3:59 PM ryan Exp $
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    public void testAdmin() throws Exception {
        rabbitAdmin.declareExchange(new DirectExchange("test.direct", false, false));

        rabbitAdmin.declareExchange(new TopicExchange("test.topic", false, false));

        rabbitAdmin.declareExchange(new FanoutExchange("test.fanout", false, false));

        rabbitAdmin.declareQueue(new Queue("test.direct.queue", false));

        rabbitAdmin.declareQueue(new Queue("test.topic.queue", false));

        rabbitAdmin.declareQueue(new Queue("test.fanout.queue", false));

        // 绑定交换机和队列写法一
        rabbitAdmin.declareBinding(new Binding("test.direct.queue", Binding.DestinationType.QUEUE, "test.direct", "direct", new HashMap<>()));

        // 绑定交换机和队列写法二
        rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue("test.topic.queue", false)) // 直接创建队列
                .to(new TopicExchange("test.topic", false, false))  // 直接创建交换机和队列关联关系
                .with("user.#")); // 指定路由key

        // 绑定交换机和队列写法二
        rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue("test.fanout.queue", false)) // 直接创建队列
                .to(new FanoutExchange("test.fanout", false, false)));  // 直接创建交换机和队列关联关系


        // 清空队列的数据
        rabbitAdmin.purgeQueue("test.topic.queue", false);
    }

    @Test
    public void testSendMessage() throws Exception {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.getHeaders().put("desc", "信息描述");
        messageProperties.getHeaders().put("type", "自定义消息类型...");
        Message message = new Message("Hello RabbitMQ".getBytes(), messageProperties);

        rabbitTemplate.convertAndSend("topic001", "spring.amqp", "hello rabbit");

        rabbitTemplate.convertAndSend("topic001", "spring.amqp", message, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                System.out.println("--- 添加额外的设置 ---");
                message.getMessageProperties().getHeaders().put("desc", "额外修改的信息描述");
                message.getMessageProperties().getHeaders().put("attr", "额外添加的属性");
                return message;
            }
        });


    }


}
