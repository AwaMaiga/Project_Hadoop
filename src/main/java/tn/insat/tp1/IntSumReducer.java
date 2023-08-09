package tn.insat.tp1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException
    {// Pour parcourir toutes les valeurs associées à la clef fournie.
        Iterator<IntWritable> i=values.iterator();
        int count=0; // Notre total pour le mot concerné.
        while(i.hasNext()) // Pour chaque valeur...
            count+=i.next().get(); // ...on l'ajoute au total.
// On renvoie le couple (clef;valeur) constitué de notre clef key
// et du total, au format Text.
      //  context.write(key, new Text(count+" occurences."));
          context.write(key, new IntWritable(count));
    }
}