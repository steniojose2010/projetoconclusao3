package dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import modelo.Pessoa;

public class PessoaDao {

    public List<Pessoa> listar() {
        List<Pessoa> pessoas = new ArrayList();
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("pessoas");
            DBCursor cursor = colecao.find();
            while (cursor.hasNext()) {
                Pessoa pessoa = new Pessoa();
                BasicDBObject pessoaDB = (BasicDBObject) cursor.next();
                pessoa.setIdPessoa(pessoaDB.getInt("_id"));
                pessoa.setNome(pessoaDB.getString("nome"));
                pessoa.setEndereco(pessoaDB.getString("endereco"));
                pessoa.setEmail(pessoaDB.getString("email"));
                pessoas.add(pessoa);
            }
            mongo.close();
            return pessoas;
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        
    }

    public Integer retornaIdPessoa() {
        Integer idPessoa = 0;
        Integer atual = 0;
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("contPessoa");
            DBCursor cursor = colecao.find();
            while (cursor.hasNext()) {
                BasicDBObject contadorDb = (BasicDBObject) cursor.next();
                atual = contadorDb.getInt("cont");
            }
            idPessoa = atual + 1;

            BasicDBObject document = new BasicDBObject();
            document.put("cont", idPessoa);
            BasicDBObject searchQuery = new BasicDBObject().append("cont", atual);
            colecao.update(searchQuery, document);
            mongo.close();
            return idPessoa;
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }

    public void gravar(Pessoa obj) {
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("pessoas");
            BasicDBObject document = new BasicDBObject();
            document.put("_id", retornaIdPessoa());
            document.put("nome", obj.getNome());
            document.put("email", obj.getEmail());
            document.put("endereco", obj.getEndereco());
            colecao.insert(document);
            mongo.close();
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }

    public Pessoa consultar(Integer codigo) {
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("pessoas");
            Pessoa pessoa = new Pessoa();
            DBCursor cursor = colecao.find();
            while (cursor.hasNext()) {
                BasicDBObject pessoaDb = (BasicDBObject) cursor.next();
                if (pessoaDb.getInt("_id") == codigo) {
                    pessoa.setIdPessoa(pessoaDb.getInt("_id"));
                    pessoa.setNome(pessoaDb.getString("nome"));
                    pessoa.setEndereco(pessoaDb.getString("endereco"));
                    pessoa.setEmail(pessoaDb.getString("email"));
                }
            }
            mongo.close();
            return pessoa;
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public boolean alterar(Pessoa obj) {
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("pessoas");
            BasicDBObject document = new BasicDBObject();
            document.put("nome", obj.getNome());
            document.put("endereco", obj.getEndereco());
            document.put("email", obj.getEmail());
            BasicDBObject searchQuery = new BasicDBObject().append("_id", obj.getIdPessoa());
            colecao.update(searchQuery, document);
            mongo.close();
            return true;
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
       
    }

    public boolean excluir(Pessoa obj) {
        DB db;
        Mongo mongo;
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            db = mongo.getDB("controlemongodb");
            char[] senha = null;
            String valor = "xhvR5WltGi_h";
            senha = valor.toCharArray();
            db.authenticate("admin", senha);
            DBCollection colecao;
            colecao = db.getCollection("pessoas");
            BasicDBObject document = new BasicDBObject();
            document.put("_id", obj.getIdPessoa());
            colecao.remove(document);
            mongo.close();
            return true;
        } catch (UnknownHostException ex) {
            Logger.getLogger(PessoaDao.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        
    }
}
