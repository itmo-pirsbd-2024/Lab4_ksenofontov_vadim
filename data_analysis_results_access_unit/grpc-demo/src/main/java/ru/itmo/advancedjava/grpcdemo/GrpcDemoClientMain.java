package ru.itmo.advancedjava.grpcdemo;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.w3c.dom.css.RGBColor;
import ru.itmo.advancedjava.grpcdemo.proto.MostFilledSectors;
import ru.itmo.advancedjava.grpcdemo.proto.UsersTrackerServiceGrpc;
import ru.itmo.advancedjava.grpcdemo.proto.Position;
import ru.itmo.advancedjava.grpcdemo.proto.UserID;
import ru.itmo.advancedjava.grpcdemo.service.UsersTrackerService;

import javax.swing.*;


import java.awt.Color;
import java.awt.Component;

import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;import java.awt.Color;
import java.awt.Component;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;


public class GrpcDemoClientMain {

    public static class MainWindow extends JFrame {

        private class SharedResourses {
            ReadWriteLock lock;

            ArrayList<Integer> pointsX;
            ArrayList<Integer> pointsY;
            MostFilledSectors sectors;
            Canvas canvas;
            JTextField useridTextField;
            JTable topSectors;

            public SharedResourses() {
                lock = new ReentrantReadWriteLock();

                pointsX = new ArrayList<>();
                pointsY = new ArrayList<>();
                sectors = MostFilledSectors.newBuilder().build();

                useridTextField = new JTextField();
                useridTextField.setMaximumSize(new Dimension(150, 150));

                topSectors = new JTable(10, 2);
                topSectors.setMaximumSize(new Dimension(150, 100));

                class TableInfoRenderer extends DefaultTableCellRenderer {
                    @Override
                    public Component getTableCellRendererComponent(JTable table, Object value,
                                                                   boolean isSelected, boolean hasFocus, int row, int column) {
                        JLabel c = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, false, row, column);

                        if (column == 0 && row == 0) c.setText("Sector");

                        if (column == 1 && row == 0) c.setText("Messages");

                        if (row <= sectors.getSectorsCount()) {
                            if (row > 0 && column == 0)
                                c.setBackground(new Color(255 - 20 * (row - 1), 100 + 30 * (row - 1), 0));
                            if (row > 0 && column == 1)
                                c.setText(String.valueOf(sectors.getSectors(sectors.getSectorsCount() - (row - 1) - 1).getMessagesGot()));
                        } else {
                            c.setBackground(new JLabel().getBackground());
                        }

                        return c;
                    }
                }

                topSectors.setDefaultRenderer(Object.class, new TableInfoRenderer());


                canvas = new Canvas() {
                    @Override
                    public void repaint() {

                        Graphics g = getGraphics();
                        g.setColor(Color.white);
                        g.fillRect(0, 0, getWidth(), getHeight());

                        // top sectors
                        var sectorColor = new Color(255, 100, 0);

                        for (var sector : sectors.getSectorsList().reversed()) {
                            g.setColor(sectorColor);
                            g.fillRect(sector.getX() * 150, sector.getY() * 150, 150, 150);
                            sectorColor = new Color(sectorColor.getRed() - 20, sectorColor.getGreen() + 30, 0);
                        }

                        topSectors.repaint();


                        // grid
                        g.setColor(Color.black);
                        for (var i = 0; i <= 600; i += 150) { // 100 represents the width in pixels between each line of the grid
                            g.drawLine(0, i, 600, i);
                            g.drawLine(i, 0, i, 600);
                        }

                        // traectory
                        int pointW = 15, pointH = 15;

                        g.setColor(Color.red);
                        Graphics2D g2 = (Graphics2D) g;
                        g2.setStroke(new BasicStroke(5));

                        for (int i = 0; i < pointsX.size(); i++) {
                            int currentPointX = (int) (((double) pointsX.get(i)) / 4000 * 600);
                            int currentPointY = (int) (((double) pointsY.get(i)) / 4000 * 600);

                            g.fillOval(currentPointX - pointW / 2, currentPointY - pointH / 2, pointW, pointH);

                            if (i > 0) {
                                int previosPointX = (int) (((double) pointsX.get(i - 1)) / 4000 * 600);
                                int previosPointY = (int) (((double) pointsY.get(i - 1)) / 4000 * 600);

                                g.drawLine(currentPointX, currentPointY, previosPointX, previosPointY);
                            }
                        }
                    }
                };

                canvas.setBackground(Color.WHITE);
                canvas.setSize(601, 601);
            }

            void processUserPositions(int x, int y) {
                lock.writeLock().lock();

                try {
                    pointsX.add(x);
                    pointsY.add(y);

                    canvas.repaint();
                } finally {
                    lock.writeLock().unlock();
                }
            }

            void processMostFilledSectors(MostFilledSectors newSectors) {
                lock.writeLock().lock();

                try {
                    sectors = newSectors;

                    canvas.repaint();
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }


        java.util.Timer timer;

        SharedResourses sharedResourses;
        UsersTrackerServiceGrpc.UsersTrackerServiceStub stubLink;

        public MainWindow(String winTitle, int w, int h) {
            super(winTitle);

            setSize(w, h);
            setLocationRelativeTo(null);

            JPanel useridFieldButtonTopTablePanel = new JPanel();
            BoxLayout useridFieldButtonTopTablePanelLayout = new BoxLayout(useridFieldButtonTopTablePanel, BoxLayout.Y_AXIS);
            useridFieldButtonTopTablePanel.setLayout(useridFieldButtonTopTablePanelLayout);

            JPanel canvasPanel = new JPanel();

            JButton button = new JButton("Sign In");


            ActionListener buttonsListener = new ButtonsListener();
            button.setActionCommand("Draw");
            button.addActionListener(buttonsListener);



            sharedResourses = new SharedResourses();

            canvasPanel.add(sharedResourses.canvas);
            canvasPanel.setBorder(BorderFactory.createLineBorder(Color.BLACK, 2));


            useridFieldButtonTopTablePanel.setAlignmentX(JComponent.CENTER_ALIGNMENT);

            useridFieldButtonTopTablePanel.add(Box.createVerticalGlue());
            useridFieldButtonTopTablePanel.add(sharedResourses.useridTextField);
            useridFieldButtonTopTablePanel.add(button);
            useridFieldButtonTopTablePanel.add(Box.createVerticalGlue());
            useridFieldButtonTopTablePanel.add(sharedResourses.topSectors);
            useridFieldButtonTopTablePanel.add(Box.createVerticalGlue());


            JPanel primaryPanel = new JPanel();
            BoxLayout primaryLayout = new BoxLayout(primaryPanel, BoxLayout.X_AXIS);
            primaryPanel.setLayout(primaryLayout);

            primaryPanel.add(useridFieldButtonTopTablePanel);
            primaryPanel.add(Box.createHorizontalGlue());
            primaryPanel.add(canvasPanel);

            Container content = getContentPane();
            content.add(BorderLayout.CENTER, primaryPanel);

            timer = new Timer();
        }

        /*TimerTask paintSectorsGrid = new TimerTask() {
            @Override
            public void run() {
                Graphics g = canvas.getGraphics();

                g.setColor(Color.black);
                for (var i = 0; i <= 600; i += 150) { // 100 represents the width in pixels between each line of the grid
                    // draw horizontal lines
                    g.drawLine(0, i, 600, i);
                    g.drawLine(i, 0, i, 600);
                }
            }
        };*/

        class ButtonsListener implements ActionListener {
            public void actionPerformed(ActionEvent e) {
                System.out.println(Integer.parseInt(sharedResourses.useridTextField.getText()));


                var channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                        .usePlaintext()
                        .build();


                stubLink = UsersTrackerServiceGrpc.newStub(channel);


                stubLink.getUserPositions(UserID.newBuilder().setId(Integer.parseInt(sharedResourses.useridTextField.getText())).build(), new StreamObserver<Position>() {
                    @Override
                    public void onNext(Position position) {
                        sharedResourses.processUserPositions(position.getX(), position.getY());
                        System.out.println(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                });


                channel.shutdown();
            }
        }
    }




    public static void main(String[] args) {
        MainWindow mainWindow = new MainWindow("Tracker Client", 780, 650);

        mainWindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        mainWindow.setResizable(false);
        mainWindow.setLocationRelativeTo(null);
        mainWindow.setVisible(true);



        new GrpcDemoClientMain().run(mainWindow);
    }

    public void run(MainWindow GUI) {
        var channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        var stub = UsersTrackerServiceGrpc.newStub(channel);

        stub.getMostFilledSectors(UserID.newBuilder().setId(0).build(), new StreamObserver<MostFilledSectors>() {
            @Override
            public void onNext(MostFilledSectors mostFilledSectors) {
                GUI.sharedResourses.processMostFilledSectors(mostFilledSectors);
                System.out.println(2);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });

        channel.shutdown();
    }
}