package my.study.boot1batch.exception;

public class MySkippableException extends RuntimeException {

    public MySkippableException(String message) {
        super(message);
    }

    public MySkippableException(String message, Throwable cause) {
        super(message, cause);
    }
}
