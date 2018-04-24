package controle;

import java.io.Serializable;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import dao.PessoaDao;
import java.util.ArrayList;
import java.util.List;
import modelo.Pessoa;
import util.utilMensagens;

@ManagedBean(name = "pessoaMB")
@SessionScoped
public class PessoaMB implements Serializable {

    private List<Pessoa> pessoas = new ArrayList();
    private String nome, endereco, email;
    private Pessoa pessoa;
    private PessoaDao pessoaDao;
    private String texto;

    public PessoaMB() {
        pessoaDao = new PessoaDao();
        pessoas = pessoaDao.listar();
    }

    public String novo() {
        pessoa = new Pessoa();
        return "new?faces-redirect=true";
    }
    
    public String novo1000() {
        pessoa = new Pessoa();
        return "newAdd?faces-redirect=true";
    }
    
    public String voltar(){
        limparVariaveis();
        return "list?faces-redirect=true";
    }
    
    public void defineObjeto(){
        pessoa.setNome(nome);
        pessoa.setEndereco(endereco);
        pessoa.setEmail(email);
    }

    public void limparVariaveis() {
        nome = null;
        endereco = null;
        email = null;
        texto=null;
    }

    public String gravar() {
            defineObjeto();
            pessoaDao.gravar(pessoa);
            pessoas = pessoaDao.listar();
            limparVariaveis();
            utilMensagens.mensagemSucesso("Pessoa cadastrada com sucesso!");
            return "list?faces-redirect=true";
    }
    
    public String alteracao(Pessoa obj) {
        pessoa = obj;
        nome = obj.getNome();
        endereco = obj.getEndereco();
        email = obj.getEmail();
        return "alter?faces-redirect=true";
    }
    
    public String alterar() {
        defineObjeto();
        if (pessoaDao.alterar(pessoa)) {
            pessoas = pessoaDao.listar();
            limparVariaveis();
            utilMensagens.mensagemSucesso("Pessoa alterada com sucesso!");
            return "list?faces-redirect=true";
        } else {
            utilMensagens.mensagemErro("Erro ao alterar pessoa!");
            return null;
        }
    }
    
    public String excluir(Pessoa obj) {
        pessoaDao = new PessoaDao();
        if (pessoaDao.excluir(obj)) {
            pessoas = pessoaDao.listar();
            utilMensagens.mensagemSucesso("Pessoa excluída com sucesso!");
            return "list?faces-redirect=true";
        } else {
            utilMensagens.mensagemErro("Erro ao excluir pessoa!");
            return null;
        }
    }
    
    public String gravar1000() {
        String[] textoSeparado = texto.split(",|,\\s|\\s\\s");
        for(int i=0; i < textoSeparado.length; i=i+3){
            pessoa = new Pessoa();
            nome=textoSeparado[i];
            endereco=textoSeparado[i+1];
            email=textoSeparado[i+2];
            pessoa.setNome(nome);
            pessoa.setEndereco(endereco);
            pessoa.setEmail(email);
            pessoaDao.gravar(pessoa);
            limparVariaveis();
        }
        pessoas=pessoaDao.listar();
        utilMensagens.mensagemSucesso("Pessoas cadastradas com sucesso!");
        return "list?faces-redirect=true";
    }

    public List<Pessoa> getPessoas() {
        return pessoas;
    }

    public void setPessoas(List<Pessoa> pessoas) {
        this.pessoas = pessoas;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getEndereco() {
        return endereco;
    }

    public void setEndereco(String endereco) {
        this.endereco = endereco;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Pessoa getPessoa() {
        return pessoa;
    }

    public void setPessoa(Pessoa pessoa) {
        this.pessoa = pessoa;
    }

    public PessoaDao getPessoaDao() {
        return pessoaDao;
    }

    public void setPessoaDao(PessoaDao pessoaDao) {
        this.pessoaDao = pessoaDao;
    }

    public String getTexto() {
        return texto;
    }

    public void setTexto(String texto) {
        this.texto = texto;
    }

}
