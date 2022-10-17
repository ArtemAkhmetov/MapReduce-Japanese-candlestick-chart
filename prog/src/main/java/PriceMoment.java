
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PriceMoment implements Writable {

    private Double price;
    private Long moment;
    private Long ID_DEAL;
    private int id_candle;

    public PriceMoment() {
        this.price = 0.0;
        this.moment = 0L;
        this.ID_DEAL = 0L;
        this.id_candle = 0;
    }

    public PriceMoment(Double price, Long moment, Long ID_DEAL, int id_candle) {
        this.price = price;
        this.moment = moment;
        this.ID_DEAL = ID_DEAL;
        this.id_candle = id_candle;
    }

    public PriceMoment(PriceMoment other) {
        this.price = other.price;
        this.moment = other.moment;
        this.ID_DEAL = other.ID_DEAL;
        this.id_candle = other.id_candle;
    }

    public void set(Double price, Long moment, Long ID_DEAL, int id_candle) {
        this.price = price;
        this.moment = moment;
        this.ID_DEAL = ID_DEAL;
        this.id_candle = id_candle;
    }

    public Double getPrice() {
        return price;
    }

    public Long getMoment() {
        return moment;
    }

    public Long getID_DEAL() { return ID_DEAL; }

    public int getId_candle() { return id_candle; }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.price = in.readDouble();
        this.moment = in.readLong();
        this.ID_DEAL = in.readLong();
        this.id_candle = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(price);
        out.writeLong(moment);
        out.writeLong(ID_DEAL);
        out.writeInt(id_candle);
    }

    @Override
    public String toString() {
        String output = "{" + price + ", " + moment + ", " + ID_DEAL + ", " + id_candle + "}";
        return output;
    }
}
