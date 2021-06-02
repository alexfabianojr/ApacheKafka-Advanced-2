package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            var orderId = UUID.randomUUID().toString();
            //var amount = new BigDecimal(Math.random() * 10000 + 1);

            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            //var email = Math.random() + "@email.com";

            var order = new Order(orderId, amount, email);
            orderDispatcher.sendAsync("ECOMMERCE_NEW_ORDER", email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

            System.out.println("New order sent successfully!");
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.getWriter().println("New order sent successfully!");

        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new ServletException();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new ServletException();
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }
}
