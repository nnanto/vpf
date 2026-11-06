//! A small SQL-like predicate language evaluated with Arrow kernels.
//!
//! Grammar (case-insensitive keywords):
//! ```text
//! expr    := or
//! or      := and (OR and)*
//! and     := not (AND not)*
//! not     := NOT not | primary
//! primary := '(' expr ')' | TRUE | FALSE
//!          | operand cmp operand
//!          | operand IS [NOT] NULL
//!          | operand [NOT] IN '(' literal (',' literal)* ')'
//!          | operand [NOT] BETWEEN operand AND operand
//!          | operand [NOT] LIKE 'pattern'
//! operand := column | "quoted column" | 'string' | number | TRUE | FALSE | NULL
//! cmp     := = | == | != | <> | < | <= | > | >=
//! ```
//! Literals are cast to the type of the column they are compared with. Rows for which the
//! predicate evaluates to NULL do not match (SQL `WHERE` semantics).

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar, StringArray, new_null_array};
use arrow::compute::kernels::boolean::{and_kleene, not, or_kleene};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::comparison::{like, nlike};
use arrow::compute::{CastOptions, cast_with_options, is_not_null, is_null};
use arrow::datatypes::DataType;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Num(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Column(String),
    Lit(Literal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Const(bool),
    Cmp(Operand, CmpOp, Operand),
    IsNull(Operand, bool),
    In(Operand, Vec<Literal>, bool),
    Like(Operand, String, bool),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    /// Double-quoted identifier (never a keyword).
    QIdent(String),
    Str(String),
    Num(String),
    Op(String),
    LParen,
    RParen,
    Comma,
}

fn tokenize(s: &str) -> Result<Vec<Tok>> {
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    let err = |m: String| Error::Invalid(format!("predicate: {m}"));
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if ch == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if ch == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if ch == '\'' || ch == '"' {
            let q = ch;
            i += 1;
            let mut v = String::new();
            loop {
                if i >= c.len() {
                    return Err(err("unterminated quote".into()));
                }
                if c[i] == q {
                    if i + 1 < c.len() && c[i + 1] == q {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(c[i]);
                i += 1;
            }
            out.push(if q == '\'' { Tok::Str(v) } else { Tok::QIdent(v) });
        } else if ch.is_ascii_digit()
            || ((ch == '-' || ch == '+' || ch == '.') && c.get(i + 1).is_some_and(|d| d.is_ascii_digit() || *d == '.'))
        {
            let st = i;
            i += 1;
            while i < c.len() {
                let d = c[i];
                if d.is_ascii_alphanumeric() || d == '.' || ((d == '-' || d == '+') && matches!(c[i - 1], 'e' | 'E')) {
                    i += 1;
                } else {
                    break;
                }
            }
            out.push(Tok::Num(c[st..i].iter().collect()));
        } else if ch.is_alphabetic() || ch == '_' {
            let st = i;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(c[st..i].iter().collect()));
        } else {
            let two: String = c[i..(i + 2).min(c.len())].iter().collect();
            if ["<=", ">=", "<>", "!=", "=="].contains(&two.as_str()) {
                out.push(Tok::Op(two));
                i += 2;
            } else if ['=', '<', '>'].contains(&ch) {
                out.push(Tok::Op(ch.to_string()));
                i += 1;
            } else {
                return Err(err(format!("unexpected character '{ch}'")));
            }
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn kw(&self, k: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(k))
    }

    fn eat_kw(&mut self, k: &str) -> bool {
        if self.kw(k) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn err<T>(&self, m: &str) -> Result<T> {
        Err(Error::Invalid(format!("predicate: {m} at token {}", self.pos + 1)))
    }

    fn expect(&mut self, t: Tok) -> Result<()> {
        if self.peek() == Some(&t) {
            self.pos += 1;
            Ok(())
        } else {
            self.err(&format!("expected {t:?}"))
        }
    }

    fn or(&mut self) -> Result<Expr> {
        let mut e = self.and()?;
        while self.eat_kw("OR") {
            e = Expr::Or(Box::new(e), Box::new(self.and()?));
        }
        Ok(e)
    }

    fn and(&mut self) -> Result<Expr> {
        let mut e = self.not()?;
        while self.eat_kw("AND") {
            e = Expr::And(Box::new(e), Box::new(self.not()?));
        }
        Ok(e)
    }

    fn not(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            return Ok(Expr::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr> {
        if self.peek() == Some(&Tok::LParen) {
            self.pos += 1;
            let e = self.or()?;
            self.expect(Tok::RParen)?;
            return Ok(e);
        }
        let lhs = self.operand()?;
        let negated = self.eat_kw("NOT");
        if !negated && self.eat_kw("IS") {
            let neg = self.eat_kw("NOT");
            if !self.eat_kw("NULL") {
                return self.err("expected NULL");
            }
            return Ok(Expr::IsNull(lhs, neg));
        }
        if self.eat_kw("IN") {
            self.expect(Tok::LParen)?;
            let mut items = Vec::new();
            loop {
                match self.operand()? {
                    Operand::Lit(l) => items.push(l),
                    Operand::Column(_) => return self.err("IN list must contain literals"),
                }
                if self.peek() == Some(&Tok::Comma) {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            self.expect(Tok::RParen)?;
            return Ok(Expr::In(lhs, items, negated));
        }
        if self.eat_kw("BETWEEN") {
            let lo = self.operand()?;
            if !self.eat_kw("AND") {
                return self.err("expected AND");
            }
            let hi = self.operand()?;
            let e = Expr::And(
                Box::new(Expr::Cmp(lhs.clone(), CmpOp::Ge, lo)),
                Box::new(Expr::Cmp(lhs, CmpOp::Le, hi)),
            );
            return Ok(if negated { Expr::Not(Box::new(e)) } else { e });
        }
        if self.eat_kw("LIKE") {
            return match self.operand()? {
                Operand::Lit(Literal::Str(p)) => Ok(Expr::Like(lhs, p, negated)),
                _ => self.err("LIKE needs a string pattern"),
            };
        }
        if negated {
            return self.err("expected IN, BETWEEN or LIKE after NOT");
        }
        if let Some(Tok::Op(o)) = self.peek().cloned() {
            self.pos += 1;
            let op = match o.as_str() {
                "=" | "==" => CmpOp::Eq,
                "!=" | "<>" => CmpOp::Ne,
                "<" => CmpOp::Lt,
                "<=" => CmpOp::Le,
                ">" => CmpOp::Gt,
                ">=" => CmpOp::Ge,
                _ => unreachable!(),
            };
            let rhs = self.operand()?;
            return Ok(Expr::Cmp(lhs, op, rhs));
        }
        match lhs {
            Operand::Lit(Literal::Bool(b)) => Ok(Expr::Const(b)),
            _ => self.err("expected comparison"),
        }
    }

    fn operand(&mut self) -> Result<Operand> {
        let t = match self.peek() {
            Some(t) => t.clone(),
            None => return self.err("unexpected end"),
        };
        self.pos += 1;
        Ok(match t {
            Tok::Str(s) => Operand::Lit(Literal::Str(s)),
            Tok::Num(n) => Operand::Lit(Literal::Num(n)),
            Tok::QIdent(s) => Operand::Column(s),
            Tok::Ident(s) if s.eq_ignore_ascii_case("NULL") => Operand::Lit(Literal::Null),
            Tok::Ident(s) if s.eq_ignore_ascii_case("TRUE") => Operand::Lit(Literal::Bool(true)),
            Tok::Ident(s) if s.eq_ignore_ascii_case("FALSE") => Operand::Lit(Literal::Bool(false)),
            Tok::Ident(s) if ["AND", "OR", "NOT", "IS", "IN", "LIKE", "BETWEEN"].iter().any(|k| s.eq_ignore_ascii_case(k)) => {
                self.pos -= 1;
                return self.err(&format!("unexpected keyword {s}"));
            }
            Tok::Ident(s) => Operand::Column(s),
            _ => {
                self.pos -= 1;
                return self.err("expected column or literal");
            }
        })
    }
}

impl Expr {
    pub fn parse(s: &str) -> Result<Expr> {
        let toks = tokenize(s)?;
        let mut p = Parser { toks, pos: 0 };
        if p.toks.is_empty() {
            return Err(Error::Invalid("predicate: empty".into()));
        }
        let e = p.or()?;
        if p.pos != p.toks.len() {
            return p.err("unexpected trailing input");
        }
        Ok(e)
    }

    /// Names of referenced columns.
    pub fn columns(&self) -> BTreeSet<String> {
        fn op(o: &Operand, out: &mut BTreeSet<String>) {
            if let Operand::Column(c) = o {
                out.insert(c.clone());
            }
        }
        let mut out = BTreeSet::new();
        let mut stack = vec![self];
        while let Some(e) = stack.pop() {
            match e {
                Expr::Const(_) => {}
                Expr::Cmp(a, _, b) => {
                    op(a, &mut out);
                    op(b, &mut out);
                }
                Expr::IsNull(a, _) | Expr::In(a, _, _) | Expr::Like(a, _, _) => op(a, &mut out),
                Expr::And(a, b) | Expr::Or(a, b) => {
                    stack.push(a);
                    stack.push(b);
                }
                Expr::Not(a) => stack.push(a),
            }
        }
        out
    }

    /// Evaluates to a mask with no nulls: `true` where the predicate holds.
    pub fn matches(&self, batch: &RecordBatch) -> Result<Vec<bool>> {
        let m = self.eval(batch)?;
        Ok((0..m.len()).map(|i| m.is_valid(i) && m.value(i)).collect())
    }

    fn eval(&self, b: &RecordBatch) -> Result<BooleanArray> {
        let n = b.num_rows();
        Ok(match self {
            Expr::Const(v) => BooleanArray::from(vec![*v; n]),
            Expr::And(x, y) => and_kleene(&x.eval(b)?, &y.eval(b)?)?,
            Expr::Or(x, y) => or_kleene(&x.eval(b)?, &y.eval(b)?)?,
            Expr::Not(x) => not(&x.eval(b)?)?,
            Expr::IsNull(o, neg) => {
                let a = column_or_err(b, o)?;
                if *neg { is_not_null(&a)? } else { is_null(&a)? }
            }
            Expr::Cmp(l, op, r) => {
                let (la, ra) = match (l, r) {
                    (Operand::Column(_), Operand::Column(_)) => {
                        let la = column_or_err(b, l)?;
                        let ra = cast_with_options(&column_or_err(b, r)?, la.data_type(), &strict())?;
                        return Ok(compare(&la, *op, &ra)?);
                    }
                    (Operand::Column(_), Operand::Lit(x)) => {
                        let la = column_or_err(b, l)?;
                        let ra = literal_array(x, la.data_type())?;
                        (la, ra)
                    }
                    (Operand::Lit(x), Operand::Column(_)) => {
                        // Normalize `lit op col` to `col op' lit`.
                        let ra = column_or_err(b, r)?;
                        let la = literal_array(x, ra.data_type())?;
                        let flipped = match op {
                            CmpOp::Lt => CmpOp::Gt,
                            CmpOp::Le => CmpOp::Ge,
                            CmpOp::Gt => CmpOp::Lt,
                            CmpOp::Ge => CmpOp::Le,
                            o => *o,
                        };
                        return Ok(compare(&ra, flipped, &Scalar::new(la))?);
                    }
                    _ => return Err(Error::Invalid("predicate: comparison needs a column".into())),
                };
                compare(&la, *op, &Scalar::new(ra))?
            }
            Expr::In(o, items, neg) => {
                let a = column_or_err(b, o)?;
                let mut acc = BooleanArray::from(vec![false; n]);
                for it in items {
                    let lit = literal_array(it, a.data_type())?;
                    acc = or_kleene(&acc, &cmp::eq(&a, &Scalar::new(lit))?)?;
                }
                if *neg { not(&acc)? } else { acc }
            }
            Expr::Like(o, pat, neg) => {
                let a = cast_with_options(&column_or_err(b, o)?, &DataType::Utf8, &CastOptions::default())?;
                let p = Scalar::new(StringArray::from(vec![pat.as_str()]));
                if *neg { nlike(&a, &p)? } else { like(&a, &p)? }
            }
        })
    }
}

/// Fetches a referenced column, decoding dictionary arrays to their values.
fn column_or_err(b: &RecordBatch, o: &Operand) -> Result<ArrayRef> {
    let Operand::Column(name) = o else {
        return Err(Error::Invalid("predicate: expected a column".into()));
    };
    let a = b.column_by_name(name).ok_or_else(|| Error::NotFound(format!("column '{name}'")))?.clone();
    Ok(match a.data_type() {
        DataType::Dictionary(_, v) => cast_with_options(&a, v, &CastOptions::default())?,
        _ => a,
    })
}

fn strict() -> CastOptions<'static> {
    CastOptions { safe: false, ..Default::default() }
}

/// A single-element array holding `lit` cast to `ty`.
pub fn literal_array(lit: &Literal, ty: &DataType) -> Result<ArrayRef> {
    let src: ArrayRef = match lit {
        Literal::Null => return Ok(new_null_array(ty, 1)),
        Literal::Str(s) | Literal::Num(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Literal::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
    };
    cast_with_options(&src, ty, &strict())
        .map_err(|e| Error::Invalid(format!("predicate literal {lit:?} is not a valid {ty}: {e}")))
}

fn compare(
    l: &dyn arrow::array::Datum,
    op: CmpOp,
    r: &dyn arrow::array::Datum,
) -> std::result::Result<BooleanArray, arrow::error::ArrowError> {
    match op {
        CmpOp::Eq => cmp::eq(l, r),
        CmpOp::Ne => cmp::neq(l, r),
        CmpOp::Lt => cmp::lt(l, r),
        CmpOp::Le => cmp::lt_eq(l, r),
        CmpOp::Gt => cmp::gt(l, r),
        CmpOp::Ge => cmp::gt_eq(l, r),
    }
}
