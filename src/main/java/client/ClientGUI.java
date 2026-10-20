/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import com.cs324.distrilab.grpc.JobServiceGrpc;
import com.cs324.distrilab.grpc.JobUpdate;
import com.cs324.distrilab.grpc.MaxJobRequest;
import com.cs324.distrilab.grpc.PrimeSumJobRequest;
import com.cs324.distrilab.grpc.PrimeCountJobRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.UUID;

/**
 *
 * @author gound
 */
public class ClientGUI extends JFrame {

    private JComboBox<String> jobType;
    private JTextField inputField;
    private JButton submitButton;
    private JButton loadCsvButton;
    private JTextArea resultArea;
    private JProgressBar progressBar;
    private JLabel progressLabel;
    private JTextArea workerProgressArea;

    public ClientGUI() {

        setTitle("CS324 Distributed System - Client");
        setSize(600, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Main panel
        JPanel mainPanel = new JPanel(new BorderLayout(10, 10));

        mainPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

        // Job selection
        JPanel topPanel = new JPanel(new FlowLayout());

        JLabel jobLabel = new JLabel("Select Job:");

        jobType = new JComboBox<>(new String[]{
            "MAX",
            "PRIMESUM",
            "PRIMECOUNT"
        });

        topPanel.add(jobLabel);
        topPanel.add(jobType);

        
        // Input section
        JPanel inputPanel = new JPanel(new BorderLayout(5, 5));

        JLabel inputLabel = new JLabel("Input:");

        inputField = new JTextField();

        inputPanel.add(inputLabel, BorderLayout.WEST);
        inputPanel.add(inputField, BorderLayout.CENTER);

        
        // Buttons
        JPanel buttonPanel = new JPanel(new FlowLayout());

        loadCsvButton = new JButton("Load CSV");
        submitButton = new JButton("Submit Job");

        buttonPanel.add(loadCsvButton);
        buttonPanel.add(submitButton);
        
        // Overall job progress
        progressBar = new JProgressBar(0, 100);
        progressBar.setValue(0);
        progressBar.setStringPainted(true);
        progressBar.setString("Waiting for Job");
        
        progressBar.setToolTipText("Click to view worker-level progress");

        progressBar.setCursor(
                Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        progressBar.addMouseListener(new java.awt.event.MouseAdapter() {
            
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                String details = workerProgressArea.getText();

                JOptionPane.showMessageDialog(
                        ClientGUI.this,
                        details,
                        "Worker-Level Progress",
                        JOptionPane.INFORMATION_MESSAGE);
            }
        });

        progressLabel = new JLabel("Progress: 0%");

        // Individual worker progress
        workerProgressArea = new JTextArea(3, 35);
        workerProgressArea.setEditable(false);
        workerProgressArea.setText("Worker progress will appear here.");
        workerProgressArea.setFont(
                new Font(Font.MONOSPACED, Font.PLAIN, 12)
        );

        JScrollPane workerScrollPane =
                new JScrollPane(workerProgressArea);

        // Result area
        resultArea = new JTextArea();

        resultArea.setEditable(false);

        JScrollPane scrollPane = new JScrollPane(resultArea);

        // Arrange the input and progress panels
        JPanel workPanel = new JPanel(new BorderLayout(5, 10));
        workPanel.add(inputPanel, BorderLayout.NORTH);
        
        JPanel progressPanel = new JPanel(new BorderLayout(5, 5));

        JPanel progressHeader = new JPanel(new BorderLayout());
        progressHeader.add(progressLabel, BorderLayout.NORTH);
        progressHeader.add(progressBar, BorderLayout.CENTER);

        progressPanel.add(progressHeader, BorderLayout.NORTH);
        progressPanel.add(workerScrollPane, BorderLayout.CENTER);
        progressPanel.add(buttonPanel, BorderLayout.SOUTH);
        
        workPanel.add(progressPanel, BorderLayout.CENTER);
        mainPanel.add(topPanel, BorderLayout.NORTH);
        mainPanel.add(workPanel, BorderLayout.CENTER);
        add(mainPanel, BorderLayout.NORTH);
        add(scrollPane, BorderLayout.CENTER);

        // Button actions
        submitButton.addActionListener(
                e -> submitJob()
        );

        loadCsvButton.addActionListener(
                e -> loadCsvFile()
        );
    }

    
    //Loads numbers from a CSV file.
    private void loadCsvFile() {

        JFileChooser fileChooser = new JFileChooser();

        int result = fileChooser.showOpenDialog(this);

        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File file = fileChooser.getSelectedFile();

        try {

            List<String> numbers = new ArrayList<>();

            BufferedReader reader = new BufferedReader(
                         new FileReader(file)
                    );

            String line;

            while ((line = reader.readLine()) != null) {

                String[] values = line.split(",");

                for (String value : values) {

                    value = value.trim();

                    if (!value.isEmpty()) {
                        numbers.add(value);
                    }
                }
            }

            reader.close();

            if (numbers.isEmpty()) {

                JOptionPane.showMessageDialog(
                        this,"The CSV file contains no numbers."
                );

                return;
            }

            // Put the CSV numbers into the input field.
            inputField.setText(
                    String.join(",", numbers)
            );

            resultArea.append(
                    "CSV loaded: "
                    + file.getName()
                    + "\n"
            );

        } catch (Exception e) {

            JOptionPane.showMessageDialog(
                    this,
                    "Could not load CSV file: "
                    + e.getMessage()
            );
        }
    }

    
     //Sends the selected job to the
     //distributed worker system.
     
    private void submitJob() {

        String selectedJob = (String) jobType.getSelectedItem();
        String input = inputField.getText().trim();

        if (input.isEmpty()) {
            resultArea.append("Please enter some input.\n");
            return;
        }

        // Prevent repeated submissions while this job is running.
        submitButton.setEnabled(false);
        progressBar.setValue(0);
        progressBar.setString("Submitting...");
        updateProgress(0, "Submitting...");
        workerProgressArea.setText("Waiting for worker updates...\n");

        new Thread(() -> {

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", 50051)
                    .usePlaintext()
                    .build();

            try {
                JobServiceGrpc.JobServiceBlockingStub stub =
                        JobServiceGrpc.newBlockingStub(channel);

                String jobId = UUID.randomUUID().toString();
                Iterator<JobUpdate> updates;

                if ("MAX".equals(selectedJob)) {

                    int[] numbers = parseNumbers(input);

                    MaxJobRequest request = MaxJobRequest.newBuilder()
                            .setJobId(jobId)
                            .addAllNumbers(
                                    java.util.Arrays.stream(numbers)
                                            .boxed()
                                            .toList()
                            )
                            .build();

                    updates = stub.submitMax(request);

                } else if ("PRIMESUM".equals(selectedJob)) {

                    String[] values = input.split(",");

                    if (values.length != 2) {
                        throw new IllegalArgumentException(
                                "PRIMESUM format must be: start,end"
                        );
                    }

                    int start = Integer.parseInt(values[0].trim());
                    int end = Integer.parseInt(values[1].trim());

                    if (start > end) {
                        throw new IllegalArgumentException(
                                "Start must be less than or equal to end."
                        );
                    }

                    PrimeSumJobRequest request = PrimeSumJobRequest.newBuilder()
                                    .setJobId(jobId)
                                    .setStart(start)
                                    .setEnd(end)
                                    .build();

                    updates = stub.submitPrimeSum(request);

                } else if ("PRIMECOUNT".equals(selectedJob)) {

                    int[] numbers = parseNumbers(input);

                    PrimeCountJobRequest request =
                            PrimeCountJobRequest.newBuilder()
                                    .setJobId(jobId)
                                    .addAllNumbers(
                                            java.util.Arrays.stream(numbers)
                                                    .boxed()
                                                    .toList()
                                    )
                                    .build();

                    updates = stub.submitPrimeCount(request);

                } else {
                    throw new IllegalArgumentException(
                            "Unknown job type: " + selectedJob
                    );
                }

                boolean receivedResult = false;

                while (updates.hasNext()) {
                    JobUpdate update = updates.next();

                    int percentage = (int) Math.round(
                            update.getOverallPercentage()
                    );
                    
                    SwingUtilities.invokeLater(() -> {
                    StringBuilder workerText = new StringBuilder();

                    for (var worker : update.getWorkerProgressList()) {
                        workerText.append("Worker ")
                                .append(worker.getWorkerId())
                                .append(": ")
                                .append(worker.getCompletedUnits())
                                .append("/")
                                .append(worker.getTotalUnits())
                                .append(" units - ")
                                .append(worker.getStatus())
                                .append("\n");
                    }

                    if (workerText.length() > 0) {
                        workerProgressArea.setText(workerText.toString());
                    }
                });

                    updateProgress(
                            percentage,
                            update.getCompleted()
                                    ? "Completed"
                                    : "Processing..."
                    );

                    if (update.getCompleted()) {
                        receivedResult = true;

                        SwingUtilities.invokeLater(() ->
                                resultArea.append(
                                        selectedJob + " Result: "
                                        + update.getResult() + "\n"
                                )
                        );
                    }
                }

                if (!receivedResult) {
                    SwingUtilities.invokeLater(() ->
                            resultArea.append(
                                    selectedJob
                                    + ": Stream ended without a final result.\n"
                            )
                    );
                }

            } catch (Exception e) {

                SwingUtilities.invokeLater(() -> {
                    resultArea.append(
                            "Error: " + e.getMessage() + "\n"
                    );
                    updateProgress(0, "Failed");
                });

            } finally {
                channel.shutdown();

                SwingUtilities.invokeLater(() ->
                        submitButton.setEnabled(true)
                );
            }

        }).start();
    }
    
    // Updates the progress bar safely from another thread.
    private void updateProgress(int percentage, String status) {
        int safePercentage = Math.max(0, Math.min(100, percentage));

        SwingUtilities.invokeLater(() -> {
            progressBar.setValue(safePercentage);
            progressLabel.setText("Progress: " + safePercentage + "%");
            progressBar.setString(status);
        });
    }

    /**
     * Converts comma-separated numbers into
     * an integer array.
     */
    private int[] parseNumbers(String input) {

        String[] values =
                input.split(",");

        int[] numbers =
                new int[values.length];

        for (int i = 0;
                i < values.length;
                i++) {

            numbers[i] =
                    Integer.parseInt(
                            values[i].trim()
                    );
        }

        return numbers;
    }

    
     //Main method used to start the GUI.
     
    public static void main(String[] args) {

        SwingUtilities.invokeLater(() -> {

            ClientGUI gui = new ClientGUI();

            gui.setVisible(true);
        });
    }
}