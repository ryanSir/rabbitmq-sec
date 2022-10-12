package com.ryan.rabbitmq;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

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
}
