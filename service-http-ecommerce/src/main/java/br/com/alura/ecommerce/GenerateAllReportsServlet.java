package br.com.alura.ecommerce;

import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.servlet.Source;

import java.io.IOException;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            batchDispatcher.sendAsync("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                    "ECOMMERCE_USER_GENERATE_READING_REPORT",
                    new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()),
                    "ECOMMERCE_USER_GENERATE_READING_REPORT");

            System.out.println("Sent generate report to all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated");
        } catch (Exception e) {
            throw new ServletException();
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }
}
