//! Compare written nonnegative decimals without rounding or a fixed exponent range.
use std::cmp::Ordering;

pub(super) fn compare(left: &str, right: &str) -> Ordering {
    let left = parts(left);
    let right = parts(right);
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some((left_power, left)), Some((right_power, right))) => {
            left_power.compare(&right_power).then_with(|| {
                left.bytes()
                    .chain(std::iter::repeat(b'0'))
                    .take(left.len().max(right.len()))
                    .cmp(
                        right
                            .bytes()
                            .chain(std::iter::repeat(b'0'))
                            .take(left.len().max(right.len())),
                    )
            })
        }
    }
}

fn parts(text: &str) -> Option<(Power, String)> {
    let (written, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
    let (whole, fraction) = written.split_once('.').unwrap_or((written, ""));
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return None;
    }
    let adjustment = digits.len() as i128 - fraction.len() as i128;
    Some((Power::new(exponent).add(adjustment), digits.into()))
}

struct Power {
    negative: bool,
    digits: String,
}
impl Power {
    fn new(text: &str) -> Self {
        let negative = text.starts_with('-');
        let digits = text.trim_start_matches(['-', '+']).trim_start_matches('0');
        Self {
            negative: negative && !digits.is_empty(),
            digits: if digits.is_empty() {
                "0".into()
            } else {
                digits.into()
            },
        }
    }
    fn compare(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => {
                let order = compare_digits(&self.digits, &other.digits);
                if self.negative {
                    order.reverse()
                } else {
                    order
                }
            }
        }
    }
    fn add(self, amount: i128) -> Self {
        let other = Self::new(&amount.to_string());
        if self.negative == other.negative {
            Self {
                negative: self.negative,
                digits: add_digits(&self.digits, &other.digits),
            }
        } else {
            match compare_digits(&self.digits, &other.digits) {
                Ordering::Equal => Self::new("0"),
                Ordering::Greater => Self {
                    negative: self.negative,
                    digits: subtract_digits(&self.digits, &other.digits),
                },
                Ordering::Less => Self {
                    negative: other.negative,
                    digits: subtract_digits(&other.digits, &self.digits),
                },
            }
        }
    }
}
fn compare_digits(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}
fn add_digits(left: &str, right: &str) -> String {
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    let mut result = Vec::new();
    let mut carry = 0;
    loop {
        let (left_digit, right_digit) = (left.next(), right.next());
        if left_digit.is_none() && right_digit.is_none() && carry == 0 {
            break;
        }
        let sum = left_digit.map_or(0, |byte| byte - b'0')
            + right_digit.map_or(0, |byte| byte - b'0')
            + carry;
        result.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    result.reverse();
    String::from_utf8(result).expect("decimal digits")
}
fn subtract_digits(left: &str, right: &str) -> String {
    let mut right = right.bytes().rev();
    let mut borrow = 0;
    let mut result = Vec::new();
    for digit in left.bytes().rev() {
        let difference = i16::from(digit - b'0')
            - i16::from(right.next().map_or(0, |byte| byte - b'0'))
            - borrow;
        borrow = i16::from(difference < 0);
        result.push(b'0' + (difference + borrow * 10) as u8);
    }
    while result.len() > 1 && result.last() == Some(&b'0') {
        result.pop();
    }
    result.reverse();
    String::from_utf8(result).expect("decimal digits")
}

#[cfg(test)]
mod tests;
